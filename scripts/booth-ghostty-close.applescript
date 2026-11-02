-- Close the Ghostty window booth-launch opened (see booth-ghostty.applescript).
--   osascript scripts/booth-ghostty-close.applescript [window id]
-- Closes the window with the given ID, and any window whose tabs include "Session manager" and
-- "Crash" (a window opened before its ID was saved).
on run argv
	set savedId to ""
	if (count of argv) > 0 then set savedId to item 1 of argv
	set closed to 0
	tell application "Ghostty"
		repeat with w in (windows as list)
			set tabNames to name of every tab of w
			if (id of w) is savedId or (tabNames contains "Session manager" and tabNames contains "Crash") then
				close window w
				set closed to closed + 1
			end if
		end repeat
	end tell
	return "closed " & closed & " booth window(s)"
end run
