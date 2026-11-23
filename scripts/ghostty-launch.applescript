-- Open one Ghostty window with a titled tab per demo process (Ghostty 1.3+ AppleScript).
--   osascript scripts/ghostty-launch.applescript <project dir> [dry]
-- Each tab is an ordinary shell with its command typed in, so Ctrl-C stops the process and
-- leaves the tab usable. "dry" types an echo of each command instead of running it.
-- Returns the window's ID, which `just launch` saves so `just stop` can close the window.
on run argv
	set projectDir to item 1 of argv
	set dryRun to (count of argv) > 1 and item 2 of argv is "dry"
	set tabSpecs to {¬
		{"Temporal", "just temporal"}, ¬
		{"Session manager", "just _wait-temporal && just session-manager"}, ¬
		{"Sandboxes", "just up && just status"}, ¬
		{"Agent Kit UI", "just _wait-temporal && just _open-pages-when-up & just _wait-temporal && just server"}, ¬
		{"Ticker", "just _wait-sandboxes && just ticker"}, ¬
		{"Crash", "just _wait-sandboxes && just crash-help"}}
	tell application "Ghostty"
		activate
		set w to missing value
		repeat with spec in tabSpecs
			set theCommand to item 2 of spec
			if dryRun then set theCommand to "echo 'would run: " & theCommand & "'"
			set cfg to new surface configuration
			set initial working directory of cfg to projectDir
			set initial input of cfg to theCommand & return
			if w is missing value then
				set w to new window with configuration cfg
				set t to selected tab of w
			else
				set t to new tab in w with configuration cfg
			end if
			perform action ("set_tab_title:" & (item 1 of spec)) on focused terminal of t
		end repeat
		select tab (tab 1 of w)
		return id of w
	end tell
end run
