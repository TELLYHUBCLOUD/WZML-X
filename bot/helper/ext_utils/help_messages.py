# ruff: noqa: F403, F405
mirror = """<blockquote expandable><b>📥 Send link along with command line or</b>

<code>/cmd link</code>

<b>📎 By replying to link/file:</b>

<code>/cmd -n new name -e -up upload destination</code>

<b>⚠️ NOTE:</b>
╰● Commands that start with <b>qb</b> are ONLY for torrents.</blockquote>"""

yt = """<blockquote expandable><b>📺 Send link along with command line:</b>

<code>/cmd link</code>

<b>📎 By replying to link:</b>

<code>/cmd -n new name -z password -opt x:y|x1:y1</code>

╞● Check here all supported <a href='https://github.com/yt-dlp/yt-dlp/blob/master/supportedsites.md'>SITES</a>
╞● Check all yt-dlp api options from this <a href='https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py#L212'>FILE</a>
╰● Use this <a href='https://t.me/mltb_official_channel/177'>script</a> to convert cli arguments to api options.</blockquote>"""

clone = """<blockquote expandable><b>♻️ Send Gdrive|Gdot|Filepress|Filebee|Appdrive|Gdflix link or rclone path along with command or by replying to the link/rc_path by command.</b>

╰● Use <code>-sync</code> to use sync method in rclone.

<b>Example:</b> <code>/cmd rcl/rclone_path -up rcl/rclone_path/rc -sync</code></blockquote>"""

new_name = """<blockquote expandable><b>📝 New Name:</b> <code>-n</code>

<code>/cmd link -n new name</code>

╰● <b>Note:</b> Doesn't work with torrents</blockquote>"""

multi_link = """<blockquote expandable><b>🔗 Multi links only by replying to first link/file:</b> <code>-i</code>

<code>/cmd -i 10</code> (number of links/files)</blockquote>"""

same_dir = """<blockquote expandable><b>📁 Move file(s)/folder(s) to new folder:</b> <code>-m</code>

You can use this arg also to move multiple links/torrents contents to the same directory, so all links will be uploaded together as one task

╞● <code>/cmd link -m new folder</code> (only one link inside new folder)
╞● <code>/cmd -i 10 -m folder name</code> (all links contents in one folder)
╰● <code>/cmd -b -m folder name</code> (reply to batch of message/file)

<b>While using bulk you can also use this arg with different folder name:</b>

<code>link1 -m folder1
link2 -m folder1
link3 -m folder2
link4 -m folder2
link5 -m folder3
link6</code>

╞● link1 and link2 content will be uploaded from same folder which is folder1
╞● link3 and link4 content will be uploaded from same folder also which is folder2
╞● link5 will uploaded alone inside new folder named folder3
╰● link6 will get uploaded normally alone</blockquote>"""

thumb = """<blockquote expandable><b>🖼️ Thumbnail for current task:</b> <code>-t</code>

<code>/cmd link -t tg-message-link</code> (doc or photo) or <code>none</code> (file without thumb)</blockquote>"""

split_size = """<blockquote expandable><b>✂️ Split size for current task:</b> <code>-sp</code>

<code>/cmd link -sp 500mb</code> or <code>2gb</code> or <code>4000000000</code>

╰● <b>Note:</b> Only mb and gb are supported or write in bytes without unit!</blockquote>"""

upload = """<blockquote expandable><b>📤 Upload Destination:</b> <code>-up</code>

<code>/cmd link -up rcl/gdl</code> (rcl: to select rclone config, remote & path | gdl: To select token.pickle, gdrive id) using buttons

You can directly add the upload path:
╞● <code>-up remote:dir/subdir</code>
╞● <code>-up Gdrive_id</code>
╞● <code>-up id/username</code> (telegram)
╰● <code>-up id/username|topic_id</code> (telegram)

If DEFAULT_UPLOAD is <code>rc</code> then you can pass up: <code>gd</code> to upload using gdrive tools to GDRIVE_ID.
If DEFAULT_UPLOAD is <code>gd</code> then you can pass up: <code>rc</code> to upload to RCLONE_PATH.

<b>If you want to add path or gdrive manually from your config/token:</b>
╞● Add <code>mrcc:</code> for rclone
╞● Add <code>mtp:</code> before the path/gdrive_id without space
╰● <code>/cmd link -up mrcc:main:dump</code> or <code>-up mtp:gdrive_id</code>

<b>To add leech destination:</b>
╞● <code>-up id/@username/pm</code>
╞● <code>-up b:id/@username/pm</code> (b: means leech by bot)
╞● <code>-up u:id/@username</code> (u: means leech by user)
╞● <code>-up h:id/@username</code> (hybrid leech)
╰● <code>-up id/@username|topic_id</code> (leech in specific chat and topic)

<b>In case you want to specify token.pickle or service accounts:</b>
╞● <code>tp:gdrive_id</code> (using token.pickle)
╞● <code>sa:gdrive_id</code> (using service accounts)
╰● <code>mtp:gdrive_id</code> (using token.pickle uploaded from usetting)

DEFAULT_UPLOAD doesn't affect on leech cmds.</blockquote>"""

user_download = """<blockquote expandable><b>👤 User Download:</b> link

╞● <code>/cmd tp:link</code> to download using owner token.pickle
╞● <code>/cmd sa:link</code> to download using service account
╞● <code>/cmd tp:gdrive_id</code> to download using token.pickle and file_id
╞● <code>/cmd sa:gdrive_id</code> to download using service account and file_id
╞● <code>/cmd mtp:gdrive_id</code> or <code>mtp:link</code> to download using user token.pickle
╰● <code>/cmd mrcc:remote:path</code> to download using user rclone config

You can simply edit upload using owner/user token/config from usetting without adding mtp: or mrcc: before the path/id</blockquote>"""

rcf = """<blockquote expandable><b>🚩 Rclone Flags:</b> <code>-rcf</code>

<code>/cmd link|path|rcl -up path|rcl -rcf --buffer-size:8M|--drive-starred-only|key|key:value</code>

This will override all other flags except --exclude

╰● Check here all <a href='https://rclone.org/flags/'>RcloneFlags</a></blockquote>"""

bulk = """<blockquote expandable><b>📦 Bulk Download:</b> <code>-b</code>

Bulk can be used only by replying to text message or text file contains links separated by new line.

<b>Example:</b>
<code>link1 -n new name -up remote1:path1 -rcf |key:value|key:value
link2 -z -n new name -up remote2:path2
link3 -e -n new name -up remote2:path2</code>

Reply to this example by this cmd → <code>/cmd -b</code> (bulk)

<b>⚠️ Note:</b> Any arg along with the cmd will be setted to all links

<code>/cmd -b -up remote: -z -m folder name</code> (all links contents in one zipped folder uploaded to one destination)

╞● You can't set different upload destinations along with link incase you have added -m along with cmd
╰● You can set start and end of the links from the bulk like seed, with <code>-b start:end</code> or only end by <code>-b :end</code> or only start by <code>-b start</code>

The default start is from zero(first link) to inf.</blockquote>"""

rlone_dl = """<blockquote expandable><b>☁️ Rclone Download:</b>

Treat rclone paths exactly like links

╞● <code>/cmd main:dump/ubuntu.iso</code> or <code>rcl</code> (To select config, remote and path)
╞● Users can add their own rclone from user settings
╰● If you want to add path manually from your config add <code>mrcc:</code> before the path without space

<code>/cmd mrcc:main:dump/ubuntu.iso</code>

You can simply edit using owner/user config from usetting without adding mrcc: before the path</blockquote>"""

extract_zip = """<blockquote expandable><b>🗜️ Extract/Zip:</b> <code>-e -z</code>

╞● <code>/cmd link -e password</code> (extract password protected)
╞● <code>/cmd link -z password</code> (zip password protected)
╰● <code>/cmd link -z password -e</code> (extract and zip password protected)

<b>Note:</b> When both extract and zip added with cmd it will extract first and then zip, so always extract first</blockquote>"""

join = """<blockquote expandable><b>🔗 Join Splitted Files:</b> <code>-j</code>

This option will only work before extract and zip, so mostly it will be used with -m argument (samedir)

<b>By Reply:</b>
╞● <code>/cmd -i 3 -j -m folder name</code>
╞● <code>/cmd -b -j -m folder name</code>
╰● If u have link(folder) have splitted files: <code>/cmd link -j</code></blockquote>"""

tg_links = """<blockquote expandable><b>📱 TG Links:</b>

Treat links like any direct link
Some links need user access so you must add USER_SESSION_STRING for it.

<b>Three types of links:</b>
╞● <b>Public:</b> <code>https://t.me/channel_name/message_id</code>
╞● <b>Private:</b> <code>tg://openmessage?user_id=xxxxxx&message_id=xxxxx</code>
╞● <b>Super:</b> <code>https://t.me/c/channel_id/message_id</code>
╰● <b>Range:</b> <code>https://t.me/channel_name/first_message_id-last_message_id</code>

<b>Range Example:</b>
<code>tg://openmessage?user_id=xxxxxx&message_id=555-560</code>
or
<code>https://t.me/channel_name/100-150</code>

<b>⚠️ Note:</b> Range link will work only by replying cmd to it</blockquote>"""

sample_video = """<blockquote expandable><b>🎬 Sample Video:</b> <code>-sv</code>

Create sample video for one video or folder of videos.

╞● <code>/cmd -sv</code> (it will take the default values which 60sec sample duration and part duration is 4sec)
╰● You can control those values. Example: <code>/cmd -sv 70:5</code> (sample-duration:part-duration) or <code>/cmd -sv :5</code> or <code>/cmd -sv 70</code></blockquote>"""

screenshot = """<blockquote expandable><b>📸 ScreenShots:</b> <code>-ss</code>

Create screenshots for one video or folder of videos.

╞● <code>/cmd -ss</code> (it will take the default values which is 10 photos)
╰● You can control this value. Example: <code>/cmd -ss 6</code></blockquote>"""

seed = """<blockquote expandable><b>🌱 Bittorrent seed:</b> <code>-d</code>

<code>/cmd link -d ratio:seed_time</code> or by replying to file/link

To specify ratio and seed time add <code>-d ratio:time</code>

<b>Example:</b>
╞● <code>-d 0.7:10</code> (ratio and time)
╞● <code>-d 0.7</code> (only ratio)
╰● <code>-d :10</code> (only time) where time in minutes</blockquote>"""

zip_arg = """<blockquote expandable><b>🗜️ Zip:</b> <code>-z password</code>

╞● <code>/cmd link -z</code> (zip)
╰● <code>/cmd link -z password</code> (zip password protected)</blockquote>"""

qual = """<blockquote expandable><b>🎥 Quality Buttons:</b> <code>-s</code>

In case default quality added from yt-dlp options using format option and you need to select quality for specific link or links with multi links feature.

╰● <code>/cmd link -s</code></blockquote>"""

yt_opt = """<blockquote expandable><b>⚙️ Options:</b> <code>-opt</code>

<code>/cmd link -opt {"format": "bv*+mergeall[vcodec=none]", "nocheckcertificate": True, "playliststart": 10, "fragment_retries": float("inf"), "matchtitle": "S13", "writesubtitles": True, "live_from_start": True, "postprocessor_args": {"ffmpeg": ["-threads", "4"]}, "wait_for_video": (5, 100), "download_ranges": [{"start_time": 0, "end_time": 10}]}</code>

╞● Check all yt-dlp api options from this <a href='https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py#L184'>FILE</a>
╰● Use this <a href='https://t.me/mltb_official_channel/177'>script</a> to convert cli arguments to api options</blockquote>"""

convert_media = """<blockquote expandable><b>🔄 Convert Media:</b> <code>-ca -cv</code>

╞● <code>/cmd link -ca mp3 -cv mp4</code> (convert all audios to mp3 and all videos to mp4)
╞● <code>/cmd link -ca mp3</code> (convert all audios to mp3)
╞● <code>/cmd link -cv mp4</code> (convert all videos to mp4)
╞● <code>/cmd link -ca mp3 + flac ogg</code> (convert only flac and ogg audios to mp3)
╰● <code>/cmd link -cv mkv - webm flv</code> (convert all videos to mp4 except webm and flv)</blockquote>"""

force_start = """<blockquote expandable><b>⚡ Force Start:</b> <code>-f -fd -fu</code>

╞● <code>/cmd link -f</code> (force download and upload)
╞● <code>/cmd link -fd</code> (force download only)
╰● <code>/cmd link -fu</code> (force upload directly after download finish)</blockquote>"""

gdrive = """<blockquote expandable><b>📂 Gdrive:</b> link

If DEFAULT_UPLOAD is <code>rc</code> then you can pass up: <code>gd</code> to upload using gdrive tools to GDRIVE_ID.

╞● <code>/cmd gdriveLink</code> or <code>gdl</code> or <code>gdriveId -up gdl</code> or <code>gdriveId</code> or <code>gd</code>
╞● <code>/cmd tp:gdriveLink</code> or <code>tp:gdriveId -up tp:gdriveId</code> or <code>gdl</code> or <code>gd</code> (to use token.pickle if service account enabled)
╞● <code>/cmd sa:gdriveLink</code> or <code>sa:gdriveId -p sa:gdriveId</code> or <code>gdl</code> or <code>gd</code> (to use service account if service account disabled)
╰● <code>/cmd mtp:gdriveLink</code> or <code>mtp:gdriveId -up mtp:gdriveId</code> or <code>gdl</code> or <code>gd</code> (to use user token.pickle)

You can simply edit using owner/user token from usetting without adding mtp: before the id</blockquote>"""

rclone_cl = """<blockquote expandable><b>☁️ Rclone:</b> path

If DEFAULT_UPLOAD is <code>gd</code> then you can pass up: <code>rc</code> to upload to RCLONE_PATH.

╞● <code>/cmd rcl/rclone_path -up rcl/rclone_path/rc -rcf flagkey:flagvalue|flagkey|flagkey:flagvalue</code>
╞● <code>/cmd rcl</code> or <code>rclone_path -up rclone_path</code> or <code>rc</code> or <code>rcl</code>
╰● <code>/cmd mrcc:rclone_path -up rcl</code> or <code>rc</code> (to use user config)

You can simply edit using owner/user config from usetting without adding mrcc: before the path</blockquote>"""

name_swap = r"""<blockquote expandable><b>🔄 Name Substitution:</b> <code>-ns</code>

<code>/cmd link -ns script/code/s | mirror/leech | tea/ /s | clone | cpu/ | \[mltb\]/mltb | \\text\\/text/s</code>

This will affect on all files. Format: <code>wordToReplace/wordToReplaceWith/sensitiveCase</code>

Word Subtitions. You can add pattern instead of normal text. Timeout: 60 sec

<b>⚠️ NOTE:</b> You must add \ before any character, those are the characters: <code>\^$.|?*+()[]{}-</code>

╞● <code>script</code> will get replaced by <code>code</code> with sensitive case
╞● <code>mirror</code> will get replaced by <code>leech</code>
╞● <code>tea</code> will get replaced by space with sensitive case
╞● <code>clone</code> will get removed
╞● <code>cpu</code> will get replaced by space
╞● <code>[mltb]</code> will get replaced by <code>mltb</code>
╰● <code>\text\</code> will get replaced by <code>text</code> with sensitive case</blockquote>"""

transmission = """<blockquote expandable><b>📡 Tg transmission:</b> <code>-hl -ut -bt</code>

╞● <code>/cmd link -hl</code> (leech by user and bot session with respect to size) (Hybrid Leech)
╞● <code>/cmd link -bt</code> (leech by bot session)
╰● <code>/cmd link -ut</code> (leech by user)</blockquote>"""

thumbnail_layout = """<blockquote expandable><b>🖼️ Thumbnail Layout:</b> <code>-tl</code>

╰● <code>/cmd link -tl 3x3</code> (widthxheight) 3 photos in row and 3 photos in column</blockquote>"""

leech_as = """<blockquote expandable><b>📤 Leech as:</b> <code>-doc -med</code>

╞● <code>/cmd link -doc</code> (Leech as document)
╰● <code>/cmd link -med</code> (Leech as media)</blockquote>"""

ffmpeg_cmds = """<blockquote expandable><b>🎞️ FFmpeg Commands:</b> <code>-ff</code>

List of lists of ffmpeg commands. You can set multiple ffmpeg commands for all files before upload. Don't write ffmpeg at beginning, start directly with the arguments.

<b>Notes:</b>
╞● Add <code>-del</code> to the list(s) which you want from the bot to delete the original files after command run complete
╰● To execute one of pre-added lists in bot like: <code>({"subtitle": ["-i mltb.mkv -c copy -c:s srt mltb.mkv"]})</code>, you must use <code>-ff subtitle</code> (list key)

<b>Examples:</b>
<code>["-i mltb.mkv -c copy -c:s srt mltb.mkv", "-i mltb.video -c copy -c:s srt mltb", "-i mltb.m4a -c:a libmp3lame -q:a 2 mltb.mp3", "-i mltb.audio -c:a libmp3lame -q:a 2 mltb.mp3", "-i mltb -map 0:a -c copy mltb.mka -map 0:s -c copy mltb.srt"]</code>

<b>How to use mltb.* reference:</b>
╞● First cmd: input is <code>mltb.mkv</code> so this cmd will work only on mkv videos and output is <code>mltb.mkv</code> also so all outputs is mkv
╞● Second cmd: input is <code>mltb.video</code> so this cmd will work on all videos and output is only <code>mltb</code> so extension is same as input files
╞● Third cmd: input in <code>mltb.m4a</code> so this cmd will work only on m4a audios and output is <code>mltb.mp3</code> so output extension is mp3
╞● Fourth cmd: input is <code>mltb.audio</code> so this cmd will work on all audios and output is <code>mltb.mp3</code> so output extension is mp3
╰● Fifth cmd: Extract audio and subtitle streams separately</blockquote>"""

metadata = """<blockquote expandable><b>🏷️ Metadata:</b> <code>-meta</code>

Apply custom metadata to media files using pipe (|) separator.

<b>Format:</b> <code>key=value|key2=value2|key3=value3</code>

<b>🔮 Dynamic Variables:</b>
╞● <code>{filename}</code> - Original filename
╞● <code>{basename}</code> - Filename without extension  
╞● <code>{extension}</code> - File extension
╞● <code>{audiolang}</code> - Audio language (auto-detected or English)
╞● <code>{sublang}</code> - Subtitle language (auto-detected or none)
╰● <code>{year}</code> - Year extracted from filename

<b>📊 Per-Stream Metadata:</b>
Set different metadata for audio/video/subtitle streams in User Settings → FFmpeg Settings:
╞● <b>Audio Metadata:</b> Applied to each audio stream
╞● <b>Video Metadata:</b> Applied to video streams  
╰● <b>Subtitle Metadata:</b> Applied to subtitle streams

<b>📝 Examples:</b>
╞● <code>/mirror link -meta title=My Movie|artist={audiolang} Version</code>
╰● <code>/yt link -meta album={basename}|year={year}|genre=Action</code>

<b>⚠️ Escape Pipes:</b> Use <code>\\|</code> to include literal pipe in values:
╰● <code>title=Movie \\| Director's Cut</code>

<b>👤 User Settings Example:</b>
╞● Audio Metadata: <code>language={audiolang}|title=Audio Track</code>
╞● Video Metadata: <code>title={basename}|year={year}</code>
╰● Subtitle Metadata: <code>language={sublang}|title=Subtitles</code></blockquote>"""

YT_HELP_DICT = {
    "main": yt,
    "New-Name": f"{new_name}\n<b>⚠️ Note:</b> Don't add file extension",
    "Zip": zip_arg,
    "Quality": qual,
    "Options": yt_opt,
    "Multi-Link": multi_link,
    "Same-Directory": same_dir,
    "Thumb": thumb,
    "Split-Size": split_size,
    "Upload-Destination": upload,
    "Rclone-Flags": rcf,
    "Bulk": bulk,
    "Sample-Video": sample_video,
    "Screenshot": screenshot,
    "Convert-Media": convert_media,
    "Force-Start": force_start,
    "Name-Swap": name_swap,
    "TG-Transmission": transmission,
    "Thumb-Layout": thumbnail_layout,
    "Leech-Type": leech_as,
    "FFmpeg-Cmds": ffmpeg_cmds,
    "Metadata": metadata,
}

MIRROR_HELP_DICT = {
    "main": mirror,
    "New-Name": new_name,
    "DL-Auth": """<blockquote expandable><b>🔐 Direct link authorization:</b> <code>-au -ap</code>

╰● <code>/cmd link -au username -ap password</code></blockquote>""",
    "Headers": """<blockquote expandable><b>📋 Direct link custom headers:</b> <code>-h</code>

╰● <code>/cmd link -h key: value key1: value1</code></blockquote>""",
    "Extract/Zip": extract_zip,
    "Select-Files": """<blockquote expandable><b>📂 Bittorrent/JDownloader/Sabnzbd File Selection:</b> <code>-s</code>

╰● <code>/cmd link -s</code> or by replying to file/link</blockquote>""",
    "Torrent-Seed": seed,
    "Multi-Link": multi_link,
    "Same-Directory": same_dir,
    "Thumb": thumb,
    "Split-Size": split_size,
    "Upload-Destination": upload,
    "Rclone-Flags": rcf,
    "Bulk": bulk,
    "Join": join,
    "Rclone-DL": rlone_dl,
    "Tg-Links": tg_links,
    "Sample-Video": sample_video,
    "Screenshot": screenshot,
    "Convert-Media": convert_media,
    "Force-Start": force_start,
    "User-Download": user_download,
    "Name-Swap": name_swap,
    "TG-Transmission": transmission,
    "Thumb-Layout": thumbnail_layout,
    "Leech-Type": leech_as,
    "FFmpeg-Cmds": ffmpeg_cmds,
    "Metadata": metadata,
}

CLONE_HELP_DICT = {
    "main": clone,
    "Multi-Link": multi_link,
    "Bulk": bulk,
    "Gdrive": gdrive,
    "Rclone": rclone_cl,
}

RSS_HELP_MESSAGE = """<blockquote expandable><b>📡 RSS Feed Format:</b>

Use this format to add feed url:

<code>Title1 link</code> (required)
<code>Title2 link -c cmd -inf xx -exf xx</code>
<code>Title3 link -c cmd -d ratio:time -z password</code>

<code>-c command -up mrcc:remote:path/subdir -rcf --buffer-size:8M|key|key:value</code>

╞● <code>-inf</code> For included words filter
╞● <code>-exf</code> For excluded words filter
╰● <code>-stv</code> true or false (sensitive filter)

<b>Example:</b>
<code>Title https://www.rss-url.com -inf 1080 or 720 or 144p|mkv or mp4|hevc -exf flv or web|xxx</code>

This filter will parse links that its titles contain <code>(1080 or 720 or 144p) and (mkv or mp4) and hevc</code> and doesn't contain <code>(flv or web) and xxx</code> words. You can add whatever you want.

<b>Another example:</b>
<code>-inf 1080 or 720p|.web. or .webrip.|hvec or x264</code>

This will parse titles that contain <code>(1080 or 720p) and (.web. or .webrip.) and (hvec or x264)</code>. I have added space before and after 1080 to avoid wrong matching.

<b>📝 Filter Notes:</b>
╞● <code>|</code> means and
╞● Add <code>or</code> between similar keys
╞● You can add <code>or</code> and <code>|</code> as much as you want
╰● Take a look at the title if it has a static special character after or before the qualities

<b>⏱️ Timeout:</b> 60 sec</blockquote>"""

PASSWORD_ERROR_MESSAGE = """<blockquote expandable><b>🔒 This link requires a password!</b>

╞● Insert <code>::</code> after the link and write the password after the sign

╰● <b>Example:</b> <code>link::my password</code></blockquote>"""


def get_bot_commands():
    from ...core.plugin_manager import get_plugin_manager

    static_commands = {
        "Mirror": "📥 [link/file] Mirror to Upload Destination",
        "QbMirror": "🌊 [magnet/torrent] Mirror to Upload Destination using qbit",
        "Ytdl": "📺 [link] Mirror YouTube, m3u8, Social Media and yt-dlp supported urls",
        "UpHoster": "☁️ [link/file] Upload to DDL Servers",
        "Leech": "📤 [link/file] Leech files to Upload to Telegram",
        "QbLeech": "🌊 [magnet/torrent] Leech files to Upload to Telegram using qbit",
        "YtdlLeech": "📺 [link] Leech YouTube, m3u8, Social Media and yt-dlp supported urls",
        "Clone": "♻️ [link] Clone files/folders to GDrive",
        "UserSet": "👤 User personal settings",
        "ForceStart": "⚡ [gid/reply] Force start from queued task",
        "Count": "🔢 [link] Count no. of files/folders in GDrive",
        "List": "🔍 [query] Search any Text which is available in GDrive",
        "Search": "🔎 [query] Search torrents via Qbit Plugins",
        "MediaInfo": "ℹ️ [reply/link] Get MediaInfo of the Target Media",
        "Select": "📂 [gid/reply] Select files for NZB, Aria2, Qbit Tasks",
        "Ping": "📡 Ping Bot to test Response Speed",
        "Status": "📊 [id/me] Tasks Status of Bot",
        "Stats": "📈 Bot, OS, Repo & System full Statistics",
        "Rss": "📡 User RSS Management Settings",
        "IMDB": "🎬 [query] or ttxxxxxx Get IMDB info",
        "CancelAll": "❌ Cancel all Tasks on the Bot",
        "Help": "❓ Detailed help usage of the WZ Bot",
        "BotSet": "⚙️ [SUDO] Bot Management Settings",
        "Log": "📋 [SUDO] Get Bot Logs for Internal Working",
        "Restart": "🔄 [SUDO] Reboot bot",
        "RestartSessions": "🔄 [SUDO] Reboot User Sessions",
    }

    commands = static_commands.copy()

    plugin_manager = get_plugin_manager()
    if plugin_manager:
        for plugin_info in plugin_manager.list_plugins():
            if plugin_info.enabled and plugin_info.commands:
                for cmd in plugin_info.commands:
                    if cmd == "speedtest":
                        commands["SpeedTest"] = "⚡ Check Bot Speed using Speedtest.com"

    return commands


BOT_COMMANDS = get_bot_commands()


def get_help_string():
    from ..telegram_helper.bot_commands import BotCommands

    help_lines = ["<blockquote expandable><b>⚠️ NOTE:</b> Try each command without any argument to see more details.</blockquote>\n"]

    commands = BotCommands.get_commands()

    for key, cmds in commands.items():
        cmd_attr = getattr(BotCommands, f"{key}Command", None)
        if not cmd_attr:
            continue

        if isinstance(cmd_attr, list):
            cmd_str = f"/{' or /'.join(cmd_attr)}"
        else:
            cmd_str = f"/{cmd_attr}"

        if key == "SpeedTest" and key in BOT_COMMANDS:
            help_lines.append(f"╞●⚡ {cmd_str}: Check Bot Speed using Speedtest.com")
        elif key == "Mirror":
            help_lines.append(f"╞●📥 {cmd_str}: Start mirroring to cloud")
        elif key == "QbMirror":
            help_lines.append(f"╞●🌊 {cmd_str}: Start Mirroring to cloud using qBittorrent")
        elif key == "JdMirror":
            help_lines.append(f"╞●📥 {cmd_str}: Start Mirroring to cloud using JDownloader")
        elif key == "NzbMirror":
            help_lines.append(f"╞●📦 {cmd_str}: Start Mirroring to cloud using Sabnzbd")
        elif key == "Ytdl":
            help_lines.append(f"╞●📺 {cmd_str}: Mirror yt-dlp supported link")
        elif key == "UpHoster":
            help_lines.append(f"╞●☁️ {cmd_str}: Upload to DDL Servers")
        elif key == "Leech":
            help_lines.append(f"╞●📤 {cmd_str}: Start leeching to Telegram")
        elif key == "QbLeech":
            help_lines.append(f"╞●🌊 {cmd_str}: Start leeching using qBittorrent")
        elif key == "JdLeech":
            help_lines.append(f"╞●📥 {cmd_str}: Start leeching using JDownloader")
        elif key == "NzbLeech":
            help_lines.append(f"╞●📦 {cmd_str}: Start leeching using Sabnzbd")
        elif key == "YtdlLeech":
            help_lines.append(f"╞●📺 {cmd_str}: Leech yt-dlp supported link")
        elif key == "Clone":
            help_lines.append(f"╞●♻️ {cmd_str} [drive_url]: Copy file/folder to Google Drive")
        elif key == "Count":
            help_lines.append(f"╞●🔢 {cmd_str} [drive_url]: Count file/folder of Google Drive")
        elif key == "Delete":
            help_lines.append(f"╞●🗑️ {cmd_str} [drive_url]: Delete file/folder from Google Drive (Only Owner & Sudo)")
        elif key == "UserSet":
            help_lines.append(f"╞●👤 {cmd_str} [query]: Users settings")
        elif key == "BotSet":
            help_lines.append(f"╞●⚙️ {cmd_str} [query]: Bot settings")
        elif key == "Select":
            help_lines.append(f"╞●📂 {cmd_str}: Select files from torrents or nzb by gid or reply")
        elif key == "CancelTask":
            help_lines.append(f"╞●❌ {cmd_str} [gid]: Cancel task by gid or reply")
        elif key == "ForceStart":
            help_lines.append(f"╞●⚡ {cmd_str} [gid]: Force start task by gid or reply")
        elif key == "CancelAll":
            help_lines.append(f"╞●❌ {cmd_str} [query]: Cancel all [status] tasks")
        elif key == "List":
            help_lines.append(f"╞●🔍 {cmd_str} [query]: Search in Google Drive(s)")
        elif key == "Search":
            help_lines.append(f"╞●🔎 {cmd_str} [query]: Search for torrents with API")
        elif key == "MediaInfo":
            help_lines.append(f"╞●ℹ️ {cmd_str} [query]: Get media info")
        elif key == "Status":
            help_lines.append(f"╞●📊 {cmd_str}: Shows a status of all the downloads")
        elif key == "Stats":
            help_lines.append(f"╞●📈 {cmd_str}: Show stats of the machine where the bot is hosted in")
        elif key == "Ping":
            help_lines.append(f"╞●📡 {cmd_str}: Check how long it takes to Ping the Bot (Only Owner & Sudo)")
        elif key == "Authorize":
            help_lines.append(f"╞●✅ {cmd_str}: Authorize a chat or a user to use the bot (Only Owner & Sudo)")
        elif key == "UnAuthorize":
            help_lines.append(f"╞●🚫 {cmd_str}: Unauthorize a chat or a user to use the bot (Only Owner & Sudo)")
        elif key == "Users":
            help_lines.append(f"╞●👥 {cmd_str}: show users settings (Only Owner & Sudo)")
        elif key == "AddSudo":
            help_lines.append(f"╞●➕ {cmd_str}: Add sudo user (Only Owner)")
        elif key == "RmSudo":
            help_lines.append(f"╞●➖ {cmd_str}: Remove sudo users (Only Owner)")
        elif key == "Restart":
            help_lines.append(f"╞●🔄 {cmd_str}: Restart and update the bot (Only Owner & Sudo)")
        elif key == "Log":
            help_lines.append(f"╞●📋 {cmd_str}: Get a log file of the bot. Handy for getting crash reports (Only Owner & Sudo)")
        elif key == "Shell":
            help_lines.append(f"╞●💻 {cmd_str}: Run shell commands (Only Owner)")
        elif key == "AExec":
            help_lines.append(f"╞●🔧 {cmd_str}: Exec async functions (Only Owner)")
        elif key == "Exec":
            help_lines.append(f"╞●🔧 {cmd_str}: Exec sync functions (Only Owner)")
        elif key == "ClearLocals":
            help_lines.append(f"╞●🧹 /{BotCommands.ClearLocalsCommand}: Clear {BotCommands.AExecCommand} or {BotCommands.ExecCommand} locals (Only Owner)")
        elif key == "Rss":
            help_lines.append(f"╰●📡 /{BotCommands.RssCommand}: RSS Menu")

    return "\n".join(help_lines)


help_string = get_help_string()
