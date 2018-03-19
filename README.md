# pipeline_backup

Scans the current directory, or named directory, for files matching a pattern. If a valid spec file is found, it will download each component package into a path relative to its URL base.

Where `http://example.com/channel/main/linux-64/package.tar.bz2` becomes `destination/main/linux-64/package.tar.bz2` on the local filesystem.

