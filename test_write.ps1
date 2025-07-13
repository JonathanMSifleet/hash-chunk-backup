# Settings
$sourcePath = "C:\Media\Zips"
$targetPath = "S:\TestBackup"
$manifestFile = Join-Path $targetPath "manifest.json"

# Chunk size settings
$avgChunkSizeMB = 256
$maxChunkSizeMB = 1024
$avgChunkSize = $avgChunkSizeMB * 1MB
$maxChunkSize = $maxChunkSizeMB * 1MB

# Performance tracking
$performance = @{
  ReadTimes = @()
  ReadSizes = @()
  WriteTimes = @()
  WriteSizes = @()
}

function Ensure-TargetDirectory {
  param([string]$path)
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function Load-Manifest {
  param([string]$path)
  if (Test-Path $path) {
    $json = Get-Content $path -Raw | ConvertFrom-Json
    function ConvertToHashtable ($obj) {
      if ($obj -is [System.Management.Automation.PSCustomObject]) {
        $ht = @{}
        foreach ($prop in $obj.PSObject.Properties) {
          $ht[$prop.Name] = ConvertToHashtable $prop.Value
        }
        return $ht
      }
      elseif ($obj -is [System.Collections.IDictionary]) {
        $ht = @{}
        foreach ($key in $obj.Keys) {
          $ht[$key] = ConvertToHashtable $obj[$key]
        }
        return $ht
      }
      else {
        return $obj
      }
    }
    return ConvertToHashtable $json
  }
  else {
    return @{}
  }
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

function Cleanup-OrphanChunks {
  param(
    [hashtable]$manifest,
    [string]$targetPath,
    [hashtable]$counters
  )
  $validChunks = @{}
  foreach ($fileName in $manifest.Keys) {
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
    $extension = [System.IO.Path]::GetExtension($fileName)
    foreach ($chunkName in $manifest[$fileName].Keys) {
      $chunkFileName = "$baseName-$chunkName$extension.bin"
      $validChunks[$chunkFileName] = $true
    }
  }

  Get-ChildItem -Path $targetPath -Filter *.bin | ForEach-Object {
    if (-not $validChunks.ContainsKey($_.Name)) {
      Write-Host "  Removing outdated chunk: $($_.FullName)"
      Remove-Item $_.FullName -Force
      $counters["Outdated"]++
    }
  }
}

function Print-PerformanceStats {
  $totalReadTime = ($performance.ReadTimes | Measure-Object -Sum).Sum
  $totalReadBytes = ($performance.ReadSizes | Measure-Object -Sum).Sum
  $totalWriteTime = ($performance.WriteTimes | Measure-Object -Sum).Sum
  $totalWriteBytes = ($performance.WriteSizes | Measure-Object -Sum).Sum
  $totalChunks = $performance.ReadTimes.Count

  $avgReadSpeed = if ($totalReadTime -gt 0) { $totalReadBytes / $totalReadTime / 1MB } else { 0 }
  $avgWriteSpeed = if ($totalWriteTime -gt 0) { $totalWriteBytes / $totalWriteTime / 1MB } else { 0 }

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

function Process-File {
  param(
    [System.IO.FileInfo]$file,
    [hashtable]$manifest,
    [string]$targetPath,
    [int64]$avgChunkSize,
    [int64]$maxChunkSize,
    [hashtable]$counters
  )
  $fileName = $file.Name

  if (-not $manifest.ContainsKey($fileName)) {
    $manifest[$fileName] = @{}
  }

  Write-Host "Processing file: $($file.FullName)"
  $stream = [System.IO.File]::OpenRead($file.FullName)

  try {
    $chunks = @()
    $chunkIndex = 0
    $bufferSize = $avgChunkSize
    $buffer = New-Object byte[] $bufferSize

    while (($bytesRead = $stream.Read($buffer,0,$bufferSize)) -gt 0) {
      $chunkData = New-Object byte[] $bytesRead
      [System.Buffer]::BlockCopy($buffer,0,$chunkData,0,$bytesRead)

      $chunks += [pscustomobject]@{
        Index = $chunkIndex
        Data = $chunkData
      }

      $chunkIndex++

      $remainingBytes = $stream.Length - $stream.Position
      if ($remainingBytes -lt $bufferSize) {
        $bufferSize = [math]::Min($remainingBytes,$maxChunkSize)
        if ($bufferSize -le 0) { break }
        $buffer = New-Object byte[] $bufferSize
      }
    }
  }
  finally {
    $stream.Dispose()
  }

  $maxThreads = [Environment]::ProcessorCount

  Write-Host "Calculating hashes for $chunkIndex chunks in parallel with $maxThreads threads..."

  # Step 1: Calculate hashes in parallel
  $chunkResults = $chunks | ForEach-Object -Parallel {
    function Get-Hash {
      param([byte[]]$data)
      $sha256 = [System.Security.Cryptography.SHA256]::Create()
      try {
        $hashBytes = $sha256.ComputeHash($data)
        return ([System.BitConverter]::ToString($hashBytes)).Replace("-","").ToLower()
      }
      finally {
        $sha256.Dispose()
      }
    }
    [pscustomobject]@{
      Index = $_.Index
      Hash = Get-Hash $_.Data
      Data = $_.Data
    }
  } -ThrottleLimit $maxThreads

  # Step 2: Sequentially write chunks and update manifest
  foreach ($chunk in $chunkResults) {
    $chunkName = $chunk.Index.ToString() # No leading zeros now
    $chunkHash = $chunk.Hash
    $chunkData = $chunk.Data

    if (-not $manifest.ContainsKey($fileName)) {
      $manifest[$fileName] = @{}
    }

    if (-not $manifest[$fileName].ContainsKey($chunkName)) {
      Write-Host "  Creating new chunk $chunkName (Hash: $chunkHash)"
      $manifest[$fileName][$chunkName] = $chunkHash
      Write-ChunkFile -targetDir $targetPath -FileName $fileName -chunkName $chunkName -chunkData $chunkData
      $counters["New"]++
    }
    elseif ($manifest[$fileName][$chunkName] -ne $chunkHash) {
      Write-Host "  Updating chunk $chunkName (Hash changed)"
      $manifest[$fileName][$chunkName] = $chunkHash
      Write-ChunkFile -targetDir $targetPath -FileName $fileName -chunkName $chunkName -chunkData $chunkData
      $counters["Updated"]++
    }
    else {
      $counters["Unchanged"]++
    }
  }

}

# Main script

Ensure-TargetDirectory -Path $targetPath
$manifest = Load-Manifest -Path $manifestFile

$counters = @{
  new = 0
  Updated = 0
  Unchanged = 0
  Outdated = 0
}

# Get all files recursively from sourcePath
$files = Get-ChildItem -Path $sourcePath -File -Recurse

foreach ($file in $files) {
  Process-File -File $file -manifest $manifest -targetPath $targetPath -avgChunkSize $avgChunkSize -maxChunkSize $maxChunkSize -counters $counters
}

Cleanup-OrphanChunks -manifest $manifest -targetPath $targetPath -counters $counters

Save-Manifest -manifest $manifest -Path $manifestFile

Write-Host "`nSummary:"
Write-Host "  New chunks created: $($counters.New)"
Write-Host "  Chunks updated: $($counters.Updated)"
Write-Host "  Chunks unchanged: $($counters.Unchanged)"
Write-Host "  Outdated chunks removed: $($counters.Outdated)"

Print-PerformanceStats
