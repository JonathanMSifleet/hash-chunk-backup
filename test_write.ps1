# Settings
$sourcePath = "C:\Media\Zips"                        # Source drive to read files from
$targetPath = "S:\TestBackup"            # Target folder to store chunk files and manifest
$chunkSize = 1GB                           # Chunk size (1 gigabyte)
$manifestFile = Join-Path $targetPath "manifest.json"  # Manifest JSON file path

# Create target folder if it doesn't exist
if (-not (Test-Path $targetPath)) {
    New-Item -ItemType Directory -Path $targetPath -Force | Out-Null
}

# Load existing manifest or create empty one
if (Test-Path $manifestFile) {
    $json = Get-Content $manifestFile -Raw | ConvertFrom-Json
    $manifest = @{}
    foreach ($key in $json.PSObject.Properties.Name) {
        $manifest[$key] = @{}
        foreach ($subkey in $json.$key.PSObject.Properties.Name) {
            $manifest[$key][$subkey] = $json.$key.$subkey
        }
    }
} else {
    $manifest = @{}
}

# Function to compute SHA256 hash for a byte array
function Get-Hash([byte[]]$data) {
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    $hashBytes = $sha256.ComputeHash($data)
    $sha256.Dispose()
    return ([System.BitConverter]::ToString($hashBytes)).Replace("-", "").ToLower()
}

# Initialize overall counters
$totalChunks = 0
$updatedChunks = 0
$newChunks = 0
$untouchedChunks = 0

# Process each file recursively under sourcePath
Get-ChildItem -Path $sourcePath -Recurse -File | ForEach-Object {
    $file = $_
    $filePath = $file.FullName
    $fileName = $file.Name

    Write-Host "Processing file: $filePath"

    # Initialize manifest entry if missing
    if (-not $manifest.ContainsKey($fileName)) {
        $manifest[$fileName] = @{}
    }

    # Open file stream for reading
    $stream = [System.IO.File]::OpenRead($filePath)
    try {
        $chunkIndex = 0
        $buffer = New-Object byte[] $chunkSize
        while (($bytesRead = $stream.Read($buffer, 0, $chunkSize)) -gt 0) {
            # If last chunk is smaller than chunkSize, resize buffer accordingly
            if ($bytesRead -lt $chunkSize) {
                $chunkData = New-Object byte[] $bytesRead
                [Array]::Copy($buffer, 0, $chunkData, 0, $bytesRead)
            } else {
                $chunkData = $buffer
            }

            # Compute chunk hash
            $chunkHash = Get-Hash $chunkData

            # Create chunk identifier string
            $chunkName = "chunk$chunkIndex"

            # Check if chunk is new or changed
            if (-not $manifest[$fileName].ContainsKey($chunkName)) {
                Write-Host "  Creating new chunk #$chunkIndex (Hash: $chunkHash)"
                $manifest[$fileName][$chunkName] = $chunkHash

                # Save chunk binary to target folder
                $safeFileName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
                $fileExtension = [System.IO.Path]::GetExtension($fileName)
                $chunkFileName = "$safeFileName-$chunkName$($fileExtension).bin"
                $chunkFilePath = Join-Path $targetPath $chunkFileName

                [System.IO.File]::WriteAllBytes($chunkFilePath, $chunkData)

                $newChunks++
            }
            elseif ($manifest[$fileName][$chunkName] -ne $chunkHash) {
                Write-Host "  Updating chunk #$chunkIndex (Hash: $chunkHash)"
                $manifest[$fileName][$chunkName] = $chunkHash

                # Save chunk binary to target folder
                $safeFileName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
                $fileExtension = [System.IO.Path]::GetExtension($fileName)
                $chunkFileName = "$safeFileName-$chunkName$($fileExtension).bin"
                $chunkFilePath = Join-Path $targetPath $chunkFileName

                [System.IO.File]::WriteAllBytes($chunkFilePath, $chunkData)

                $updatedChunks++
            }
            else {
                Write-Host "  Chunk #$chunkIndex unchanged, skipping."
                $untouchedChunks++
            }

            $totalChunks++
            $chunkIndex++
        }
    }
    finally {
        $stream.Close()
        $stream.Dispose()
    }
}

# Save updated manifest
$manifest | ConvertTo-Json -Depth 10 | Set-Content $manifestFile

# Summary output
Write-Host "`nChunking complete."
Write-Host "Total chunks processed: $totalChunks"
Write-Host "New chunks created: $newChunks"
Write-Host "Chunks updated: $updatedChunks"
Write-Host "Chunks untouched: $untouchedChunks"