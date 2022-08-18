# magnet-to-database-or-torrent

Program takes torrent hashes as an input. In bulk, by thousand, from db. 
Downloads torrent information via libtorrent library.
Does not download any data other than .torrent files themself.
Extracts and stores info in db, optionally saves .torrent files

Currently working on trackers performance optimizations.
