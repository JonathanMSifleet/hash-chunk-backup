This is a side project of mine, with the intention to fulfill a need in my backup strategy.

I use Macrium reflect to create a maximum of 4 incremental backups of my C partition, whenever a fifth backup is about to created, Reflect will merge the two oldest backups into one.

I also use Backblaze to backup my data, but I don't want it to backup my C drive, instead I just want it to backup the images created by Macrium Reflect.

Due to how Backblaze detects file changes, whenever a 'synthetic full' backup is created by Macrium Reflect, Backblaze will see it as a new file and will upload the entire image again, which is a massive waste of bandwidth and time.

To mitigate this, these scripts will (eventually) run after each Macrium Reflect backup, which will split up my backup images into fixed-size chunks, and save them on my storage array which is automatically backed up by Backblaze.

Current logic:
- Split source files into fixed-size chunks
- Create hash of each chunk
- Save chunks to destination directory with the name being the hash
- On re-run, check if chunk already exists in destination directory
- If it exists, skip copying
- If it does not exist, create chunk with that name

This is still a work in progress, so it does not work as intended yet.

TODO:
- Fix RAM usage
- Fix file read speed
- Speed up rabin calculation
- Write current speed of file read
- Verify not deleting orphaned chunks
- Refactor
- GUI wrapper