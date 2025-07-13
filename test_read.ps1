$sourceFolder = "T:\ChunkStorage"
$targetFile = "S:\ChunkStorage\recombined_file"

# Get chunk files and sort numerically by the chunk number extracted from filename
$chunkFiles = Get-ChildItem -Path $sourceFolder -Filter "*-chunk*.mrimg.bin" | 
    Sort-Object {
        if ($_ -match "-chunk(\d+)\.") {
            [int]$matches[1]
        } else {
            0
        }
    }

Write-Host "Starting to combine $($chunkFiles.Count) chunks into $targetFile"

# Create or overwrite the target file
if (Test-Path $targetFile) {
    Remove-Item $targetFile
}

foreach ($chunk in $chunkFiles) {
    Write-Host "Appending chunk: $($chunk.Name)"
    # Read bytes from the chunk file and append to the target file
    $bytes = [System.IO.File]::ReadAllBytes($chunk.FullName)
    [System.IO.File]::OpenWrite($targetFile).Close() # make sure file exists (skip if already created)
    [System.IO.File]::Open($targetFile, [System.IO.FileMode]::Append).Write($bytes, 0, $bytes.Length)
}

Write-Host "Combining complete!"
