$sourceFolder = "S:\TestBackup"
$outputFolder = "C:\Media\ReconstructedFiles"

# Create output folder if needed
if (-not (Test-Path $outputFolder)) {
  New-Item -ItemType Directory -Path $outputFolder | Out-Null
}

# Get all chunk files
$chunkFiles = Get-ChildItem -Path $sourceFolder -Filter "*.bin"

# Group chunk files by base filename (e.g., MyFile-chunk0.mrimg.bin → MyFile.mrimg)
$groupedChunks = @{}

foreach ($file in $chunkFiles) {
  if ($file.Name -match "^(.*)-chunk(\d+)(\.[^\.]+)\.bin$") {
    $baseName = $matches[1]
    $chunkNum = [int]$matches[2]
    $extension = $matches[3]
    $originalFileName = "$baseName$extension"

    if (-not $groupedChunks.ContainsKey($originalFileName)) {
      $groupedChunks[$originalFileName] = @()
    }

    $groupedChunks[$originalFileName] += [pscustomobject]@{
      Path = $file.FullName
      ChunkIndex = $chunkNum
    }
  }
}

# Combine chunks per original file
foreach ($originalFileName in $groupedChunks.Keys) {
  $outputFile = Join-Path $outputFolder $originalFileName
  Write-Host "`nReassembling file: $originalFileName"

  # Sort chunks by index
  $orderedChunks = $groupedChunks[$originalFileName] | Sort-Object ChunkIndex

  # Create or overwrite the output file
  if (Test-Path $outputFile) {
    Remove-Item $outputFile -Force
  }

  # Append all chunks in order
  foreach ($chunk in $orderedChunks) {
    Write-Host "  Appending chunk $($chunk.ChunkIndex) → $($chunk.Path)"
    $bytes = [System.IO.File]::ReadAllBytes($chunk.Path)
    [System.IO.File]::OpenWrite($outputFile).Close() # Ensure file exists
    $stream = [System.IO.File]::Open($outputFile,[System.IO.FileMode]::Append)
    $stream.Write($bytes,0,$bytes.Length)
    $stream.Close()
  }

  Write-Host "✅ Done: $originalFileName"
}

Write-Host "`nAll files reassembled successfully!"
