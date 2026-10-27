-- Open one Ghostty window with a titled tab per booth process (Ghostty 1.3+ AppleScript).
--   osascript scripts/booth-ghostty.applescript <project dir> [dry]
-- Each tab is an ordinary shell with its command typed in, so Ctrl-C stops the process and
-- leaves the tab usable. "dry" types an echo of each command instead of running it.
on run argv
	set projectDir to item 1 of argv
	set dryRun to (count of argv) > 1 and item 2 of argv is "dry"
	set tabSpecs to {¬
		{"Temporal", "just temporal"}, ¬
		{"Session manager", "just _wait-temporal && just session-manager"}, ¬
		{"Sandboxes", "just booth-up && just status"}, ¬
		{"Booth UI", "just _wait-temporal && just _open-booth-pages-when-up & just _wait-temporal && just booth-server"}, ¬
		{"Ticker", "just _wait-sandboxes && just ticker"}}
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
		return "opened " & (count of tabs of w) & " tabs in window " & (id of w)
	end tell
end run
