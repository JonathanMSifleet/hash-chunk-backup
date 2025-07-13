# Settings
$sourcePath = "C:\Media\Zips"
$targetPath = "S:\TestBackup"
$chunkSize = 1GB
$manifestFile = Join-Path $targetPath "manifest.json"
$threads = [Environment]::ProcessorCount

# Performance tracking
$performance = @{
  ReadTimes = @()
  ReadSizes = @()
  WriteTimes = @()
  WriteSizes = @()
}

function Initialize-TargetDirectory {
  param([string]$path)
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function Get-Manifest {
  param([string]$path)
  if (Test-Path $path) {
    $json = Get-Content $path -Raw | ConvertFrom-Json
    $manifest = @{}
    foreach ($file in $json.PSObject.Properties.Name) {
      $manifest[$file] = @{}
      foreach ($chunk in $json.$file.PSObject.Properties.Name) {
        $manifest[$file][$chunk] = $json.$file.$chunk
      }
    }
    return $manifest
  }
  return @{}
}

function Save-Manifest {
  param(
    [hashtable]$manifest,
    [string]$path
  )
  $manifest | ConvertTo-Json -Depth 10 | Set-Content $path
}

function Get-Hash {
  param([byte[]]$data)
  if (-not $data -or $data.Length -eq 0) {
    throw "Get-Hash received null or empty data!"
  }
  $sha256 = [System.Security.Cryptography.SHA256]::Create()
  try {
    $hashBytes = $sha256.ComputeHash($data)
    return ([System.BitConverter]::ToString($hashBytes)).Replace("-","").ToLower()
  }
  finally {
    $sha256.Dispose()
  }
}

function Write-ChunkFile {
  param(
    [string]$targetDir,
    [string]$fileName,
    [string]$chunkName,
    [byte[]]$chunkData
  )
  $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
  $extension = [System.IO.Path]::GetExtension($fileName)
  $chunkFileName = "$baseName-$chunkName$extension.bin"
  $chunkFilePath = Join-Path $targetDir $chunkFileName
  [System.IO.File]::WriteAllBytes($chunkFilePath,$chunkData)
}

function Invoke-Chunk {
  param(
    [string]$fileName,
    [string]$chunkName,
    [byte[]]$chunkData,
    [hashtable]$manifest,
    [string]$targetPath,
    [hashtable]$counters
  )

  $chunkHash = Get-Hash $chunkData

  $writeStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

  if (-not $manifest[$fileName].ContainsKey($chunkName)) {
    Write-Host "  Creating new chunk $chunkName (Hash: $chunkHash)"
    $manifest[$fileName][$chunkName] = $chunkHash
    Write-ChunkFile -targetDir $targetPath -FileName $fileName -chunkName $chunkName -chunkData $chunkData
    $counters["New"]++
  }
  elseif ($manifest[$fileName][$chunkName] -ne $chunkHash) {
    Write-Host "  Updating chunk $chunkName (Hash: $chunkHash)"
    $manifest[$fileName][$chunkName] = $chunkHash
    Write-ChunkFile -targetDir $targetPath -FileName $fileName -chunkName $chunkName -chunkData $chunkData
    $counters["Updated"]++
  }
  else {
    Write-Host "  Chunk $chunkName unchanged, skipping."
    $writeStopwatch.Stop()
    # Skipped write, record no time or size for write
    $counters["Untouched"]++
    return
  }

  $writeStopwatch.Stop()
  $performance.WriteTimes += $writeStopwatch.Elapsed.TotalSeconds
  $performance.WriteSizes += $chunkData.Length
}

function Show-PerformanceStatsForChunk {
  param([int]$chunkIndex)

  $readTime = $performance.ReadTimes[$chunkIndex]
  $readBytes = $performance.ReadSizes[$chunkIndex]

  if ($chunkIndex -lt $performance.WriteTimes.Count) {
    $writeTime = $performance.WriteTimes[$chunkIndex]
    $writeBytes = $performance.WriteSizes[$chunkIndex]
  } else {
    $writeTime = 0
    $writeBytes = 0
  }

  $readSpeed = if ($readTime -gt 0) { $readBytes / $readTime / 1MB } else { 0 }
  $writeSpeed = if ($writeTime -gt 0) { $writeBytes / $writeTime / 1MB } else { 0 }

  $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

  Write-Host "$timestamp - Chunk $chunkIndex performance:"
  Write-Host ("  Read time: {0:N3} s, Read size: {1} bytes, Read speed: {2:N2} MB/s" -f $readTime,$readBytes,$readSpeed)
  Write-Host ("  Write time: {0:N3} s, Write size: {1} bytes, Write speed: {2:N2} MB/s" -f $writeTime,$writeBytes,$writeSpeed)
  Write-Host ""
}

function Invoke-File {
  param(
    [System.IO.FileInfo]$file,
    [hashtable]$manifest,
    [string]$targetPath,
    [int64]$chunkSize,
    [hashtable]$counters
  )
  $fileName = $file.Name

  if (-not $manifest.ContainsKey($fileName)) {
    $manifest[$fileName] = @{}
  }

  Write-Host "Processing file: $($file.FullName)"
  $stream = [System.IO.File]::OpenRead($file.FullName)
  try {
    $buffer = New-Object byte[] $chunkSize
    $chunkIndex = 0
    $allChunks = @()
    $chunkIndex = 0
    while (($bytesRead = $stream.Read($buffer,0,$chunkSize)) -gt 0) {
      $readStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

      if ($bytesRead -eq $chunkSize) {
        $chunkData = $buffer.Clone()
      } else {
        $chunkData = New-Object byte[] $bytesRead
        [System.Buffer]::BlockCopy($buffer,0,$chunkData,0,$bytesRead)
      }

      $readStopwatch.Stop()
      $performance.ReadTimes += $readStopwatch.Elapsed.TotalSeconds
      $performance.ReadSizes += $bytesRead

      $chunkName = "chunk$chunkIndex"
      $allChunks += [pscustomobject]@{
        FileName = $fileName
        ChunkName = $chunkName
        ChunkData = $chunkData
        ChunkIndex = $chunkIndex
      }

      $chunkIndex++
    }

    $stream.Dispose()

    # Process chunks in parallel
    $allChunks | ForEach-Object -Parallel {
      param($manifest,$targetPath,$counters,$performance)

      Invoke-Chunk -FileName $_.FileName -chunkName $_.ChunkName -chunkData $_.ChunkData -manifest $using:manifest -targetPath $using:targetPath -counters $using:counters
      Show-PerformanceStatsForChunk -chunkIndex $_.ChunkIndex

    } -ThrottleLimit $threads
  }
  finally {
    $stream.Dispose()
  }
}

function Remove-OrphanChunks {
  param(
    [hashtable]$manifest,
    [string]$targetPath,
    [hashtable]$counters
  )
  # Build set of valid chunk file names
  $validChunks = @{}
  foreach ($fileName in $manifest.Keys) {
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
    $extension = [System.IO.Path]::GetExtension($fileName)
    foreach ($chunkName in $manifest[$fileName].Keys) {
      $chunkFileName = "$baseName-$chunkName$extension.bin"
      $validChunks[$chunkFileName] = $true
    }
  }

  # Remove orphaned chunk files
  Get-ChildItem -Path $targetPath -Filter *.bin | ForEach-Object {
    if (-not $validChunks.ContainsKey($_.Name)) {
      Write-Host "  Removing outdated chunk: $($_.FullName)"
      Remove-Item $_.FullName -Force
      $counters["Outdated"]++
    }
  }
}

function Show-PerformanceStats {
  $totalReadTime = ($performance.ReadTimes | Measure-Object -Sum).Sum
  $totalReadBytes = ($performance.ReadSizes | Measure-Object -Sum).Sum
  $totalWriteTime = ($performance.WriteTimes | Measure-Object -Sum).Sum
  $totalWriteBytes = ($performance.WriteSizes | Measure-Object -Sum).Sum
  $totalChunks = $performance.ReadTimes.Count

  if ($totalReadTime -gt 0) {
    $avgReadSpeed = $totalReadBytes / $totalReadTime / 1MB
  } else { $avgReadSpeed = 0 }

  if ($totalWriteTime -gt 0) {
    $avgWriteSpeed = $totalWriteBytes / $totalWriteTime / 1MB
  } else { $avgWriteSpeed = 0 }

  $peakReadSpeed = 0
  for ($i = 0; $i -lt $totalChunks; $i++) {
    if ($performance.ReadTimes[$i] -gt 0) {
      $speed = $performance.ReadSizes[$i] / $performance.ReadTimes[$i] / 1MB
      if ($speed -gt $peakReadSpeed) { $peakReadSpeed = $speed }
    }
  }

  $peakWriteSpeed = 0
  for ($i = 0; $i -lt $totalChunks; $i++) {
    if ($performance.WriteTimes[$i] -gt 0) {
      $speed = $performance.WriteSizes[$i] / $performance.WriteTimes[$i] / 1MB
      if ($speed -gt $peakWriteSpeed) { $peakWriteSpeed = $speed }
    }
  }

  $avgReadTimePerChunk = if ($totalChunks -gt 0) { $totalReadTime / $totalChunks } else { 0 }
  $avgWriteTimePerChunk = if ($totalChunks -gt 0) { $totalWriteTime / $totalChunks } else { 0 }

  Write-Host "`n=== Performance Stats ==="
  Write-Host ("Total chunks processed: $totalChunks")
  Write-Host ("Total read time: {0:N2} s" -f $totalReadTime)
  Write-Host ("Total write time: {0:N2} s" -f $totalWriteTime)
  Write-Host ("Average read speed: {0:N2} MB/s" -f $avgReadSpeed)
  Write-Host ("Average write speed: {0:N2} MB/s" -f $avgWriteSpeed)
  Write-Host ("Peak read speed: {0:N2} MB/s" -f $peakReadSpeed)
  Write-Host ("Peak write speed: {0:N2} MB/s" -f $peakWriteSpeed)
  Write-Host ("Average read time per chunk: {0:N3} s" -f $avgReadTimePerChunk)
  Write-Host ("Average write time per chunk: {0:N3} s" -f $avgWriteTimePerChunk)
  Write-Host "=========================`n"
}

function Main {
  Initialize-TargetDirectory -Path $targetPath

  $manifest = Get-Manifest -Path $manifestFile

  $counters = @{
    Total = 0
    new = 0
    Updated = 0
    Untouched = 0
    Outdated = 0
  }

  $processedFiles = @{}

  # Process each file
  Get-ChildItem -Path $sourcePath -Recurse -File | ForEach-Object {
    $processedFiles[$_.Name] = $true
    Invoke-File -File $_ -manifest $manifest -targetPath $targetPath -chunkSize $chunkSize -counters $counters
  }

  # Remove manifest entries for missing files
  $manifest.Keys | Where-Object { -not $processedFiles.ContainsKey($_) } | ForEach-Object {
    Write-Host "Removing obsolete manifest entry: $_"
    $manifest.Remove($_)
  }

  Remove-OrphanChunks -manifest $manifest -targetPath $targetPath -counters $counters

  Save-Manifest -manifest $manifest -Path $manifestFile

  Write-Host "`nChunking complete."
  Write-Host "Total chunks processed: $($performance.ReadTimes.Count)"
  Write-Host "New chunks created: $($counters.New)"
  Write-Host "Chunks updated: $($counters.Updated)"
  Write-Host "Chunks untouched: $($counters.Untouched)"
  Write-Host "Chunks removed: $($counters.Outdated)"

  Show-PerformanceStats
}

# Run Main and measure total time
$scriptStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

Main

$scriptStopwatch.Stop()
Write-Host ("Total time elapsed: {0:N2} seconds" -f $scriptStopwatch.Elapsed.TotalSeconds)
