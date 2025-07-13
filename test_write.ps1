# Settings
$sourcePath = "C:\Media\Zips"
$targetPath = "S:\TestBackup"
$chunkSize = 1GB
$manifestFile = Join-Path $targetPath "manifest.json"

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

function Process-Chunk {
  param(
    [string]$fileName,
    [string]$chunkName,
    [byte[]]$chunkData,
    [hashtable]$manifest,
    [string]$targetPath,
    [hashtable]$counters
  )
  $chunkHash = Get-Hash $chunkData

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
    $counters["Untouched"]++
  }

  $counters["Total"]++
}

function Process-File {
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
    while (($bytesRead = $stream.Read($buffer,0,$chunkSize)) -gt 0) {
      if ($bytesRead -eq $chunkSize) {
        $chunkData = $buffer
      }
      else {
        $chunkData = New-Object byte[] $bytesRead
        [System.Buffer]::BlockCopy($buffer,0,$chunkData,0,$bytesRead)
      }
      $chunkName = "chunk$chunkIndex"
      Process-Chunk -FileName $fileName -chunkName $chunkName -chunkData $chunkData -manifest $manifest -targetPath $targetPath -counters $counters
      $chunkIndex++
    }
  }
  finally {
    $stream.Dispose()
  }
}

function Cleanup-OrphanChunks {
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

function Main {
  Ensure-TargetDirectory -Path $targetPath

  $manifest = Load-Manifest -Path $manifestFile

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
    Process-File -File $_ -manifest $manifest -targetPath $targetPath -chunkSize $chunkSize -counters $counters
  }

  # Remove manifest entries for missing files
  $manifest.Keys | Where-Object { -not $processedFiles.ContainsKey($_) } | ForEach-Object {
    Write-Host "Removing obsolete manifest entry: $_"
    $manifest.Remove($_)
  }

  Cleanup-OrphanChunks -manifest $manifest -targetPath $targetPath -counters $counters

  Save-Manifest -manifest $manifest -Path $manifestFile

  Write-Host "`nChunking complete."
  Write-Host "Total chunks processed: $($counters.Total)"
  Write-Host "New chunks created: $($counters.New)"
  Write-Host "Chunks updated: $($counters.Updated)"
  Write-Host "Chunks untouched: $($counters.Untouched)"
  Write-Host "Chunks removed: $($counters.Outdated)"
}


Main
