function New-OutputFolder {
    param (
        [string]$Path
    )
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Get-ChunkGroups {
    param (
        [string]$SourceFolder
    )

    $chunkFiles = Get-ChildItem -Path $SourceFolder -Filter "*.bin"
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
    return $groupedChunks
}

function Restore-File {
    param (
        [string]$OutputFolder,
        [string]$OriginalFileName,
        [array]$Chunks
    )

    $outputFile = Join-Path $OutputFolder $OriginalFileName
    Write-Host "`nRestoring file: $OriginalFileName"

    # Sort chunks by index
    $orderedChunks = $Chunks | Sort-Object ChunkIndex

    # Remove output file if exists
    if (Test-Path $outputFile) {
        Remove-Item $outputFile -Force
    }

    # Append all chunks in order
    foreach ($chunk in $orderedChunks) {
        Write-Host "  Appending chunk $($chunk.ChunkIndex) → $($chunk.Path)"
        $bytes = [System.IO.File]::ReadAllBytes($chunk.Path)

        # Ensure file exists before appending
        [System.IO.File]::OpenWrite($outputFile).Close()
        $stream = [System.IO.File]::Open($outputFile, [System.IO.FileMode]::Append)
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Close()
    }

    Write-Host "✅ Done: $OriginalFileName"
}

# --- Main Script Logic ---

$sourceFolder = "S:\TestBackup"
$outputFolder = "C:\Media\ReconstructedFiles"

New-OutputFolder -Path $outputFolder

$groupedChunks = Get-ChunkGroups -SourceFolder $sourceFolder

foreach ($originalFileName in $groupedChunks.Keys) {
    Restore-File -OutputFolder $outputFolder -OriginalFileName $originalFileName -Chunks $groupedChunks[$originalFileName]
}

Write-Host "`nAll files reassembled successfully!"
