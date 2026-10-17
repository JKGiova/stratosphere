// Terminal uses top-left coordinates; AppKit screen frames use bottom-left coordinates.
function maximizedBounds(windowBounds, screens) {
    if (!screens.length) throw new Error("No displays available");
    const primaryTop = screens[0].frame.y + screens[0].frame.height;
    let selected = screens[0];
    let largestOverlap = -1;
    for (const screen of screens) {
        const frame = screen.frame;
        const top = primaryTop - frame.y - frame.height;
        const width = Math.max(0,
            Math.min(windowBounds.x + windowBounds.width, frame.x + frame.width)
            - Math.max(windowBounds.x, frame.x));
        const height = Math.max(0,
            Math.min(windowBounds.y + windowBounds.height, top + frame.height)
            - Math.max(windowBounds.y, top));
        const overlap = width * height;
        if (overlap > largestOverlap) {
            largestOverlap = overlap;
            selected = screen;
        }
    }
    const visible = selected.visible;
    return {
        x: Math.round(visible.x),
        y: Math.round(primaryTop - visible.y - visible.height),
        width: Math.round(visible.width),
        height: Math.round(visible.height),
    };
}

function windowForTTY(windows, tty) {
    for (const window of windows) {
        for (const tab of window.tabs()) {
            if (tab.tty() === tty) return window;
        }
    }
    throw new Error("The calling terminal window was not found");
}

function run(argv) {
    const terminal = Application("com.apple.Terminal");
    const window = windowForTTY(terminal.windows(), argv[0]);
    ObjC.import("AppKit");
    const nativeScreens = $.NSScreen.screens;
    const screens = [];
    function rectangle(frame) {
        return {
            x: frame.origin.x, y: frame.origin.y,
            width: frame.size.width, height: frame.size.height,
        };
    }
    for (let index = 0; index < nativeScreens.count; index++) {
        const screen = nativeScreens.objectAtIndex(index);
        screens.push({frame: rectangle(screen.frame), visible: rectangle(screen.visibleFrame)});
    }
    window.bounds = maximizedBounds(window.bounds(), screens);
}
