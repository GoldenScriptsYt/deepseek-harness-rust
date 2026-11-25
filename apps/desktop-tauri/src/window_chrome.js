// Installed only in the main Windows webview, including the local loading page.
(() => {
  const invoke = (command, args = {}) =>
    window.__TAURI_INTERNALS__.invoke(`plugin:window|${command}`, { label: 'main', ...args })

  const install = () => {
    document.documentElement.dataset.tauriChrome = ''
    const style = document.createElement('style')
    style.textContent = `
      .dsh-window-controls { position: fixed; top: 0; right: 0; height: 40px; display: flex; z-index: 2147483646; color: var(--dsw-alias-label-primary, #eee); }
      .dsh-window-controls button { display: grid; place-items: center; width: 46px; height: 40px; padding: 0; border: 0; border-radius: 0; color: inherit; background: transparent; cursor: default; }
      .dsh-window-controls button:hover { background: var(--dsw-alias-interactive-bg-hover, #444); }
      .dsh-window-controls button:last-child:hover { color: white; background: #c42b1c; }
      .dsh-window-controls button:focus-visible { outline: 2px solid currentColor; outline-offset: -3px; }
      .dsh-window-controls svg { width: 16px; height: 16px; fill: none; stroke: currentColor; stroke-width: 1.5; stroke-linecap: round; stroke-linejoin: round; }
      .dsh-window-resize { position: fixed; z-index: 2147483647; touch-action: none; }
      .dsh-window-resize[data-edge="North"], .dsh-window-resize[data-edge="South"] { left: 6px; right: 6px; height: 5px; cursor: ns-resize; }
      .dsh-window-resize[data-edge="North"] { top: 0; } .dsh-window-resize[data-edge="South"] { bottom: 0; }
      .dsh-window-resize[data-edge="West"], .dsh-window-resize[data-edge="East"] { top: 6px; bottom: 6px; width: 5px; cursor: ew-resize; }
      .dsh-window-resize[data-edge="West"] { left: 0; } .dsh-window-resize[data-edge="East"] { right: 0; }
      .dsh-window-resize[data-edge="NorthWest"], .dsh-window-resize[data-edge="NorthEast"], .dsh-window-resize[data-edge="SouthWest"], .dsh-window-resize[data-edge="SouthEast"] { width: 6px; height: 6px; }
      .dsh-window-resize[data-edge="NorthWest"] { top: 0; left: 0; cursor: nwse-resize; }
      .dsh-window-resize[data-edge="NorthEast"] { top: 0; right: 0; cursor: nesw-resize; }
      .dsh-window-resize[data-edge="SouthWest"] { bottom: 0; left: 0; cursor: nesw-resize; }
      .dsh-window-resize[data-edge="SouthEast"] { bottom: 0; right: 0; cursor: nwse-resize; }
      html[data-tauri-chrome] [data-rightbar-collapsed] header:has(> [data-conversation-header-leading]) { padding-right: 150px; }
      html[data-tauri-chrome] [data-global-panel="true"] { padding-top: 40px; }
    `
    document.head.append(style)

    const controls = document.createElement('div')
    controls.className = 'dsh-window-controls'
    controls.setAttribute('role', 'group')
    controls.setAttribute('aria-label', 'Window controls')
    for (const [label, command, path] of [
      ['Minimize', 'minimize', 'M2 8h12'],
      ['Maximize', 'toggle_maximize', 'M3 3h10v10H3z'],
      ['Close', 'close', 'M2.5 2.5l11 11M13.5 2.5l-11 11'],
    ]) {
      const button = document.createElement('button')
      button.type = 'button'
      button.setAttribute('aria-label', label)
      button.title = label
      button.innerHTML = `<svg viewBox="0 0 16 16" aria-hidden="true"><path d="${path}" /></svg>`
      button.addEventListener('click', () => { void invoke(command).then(() => {
        if (command === 'toggle_maximize') void updateMaximize()
      }) })
      controls.append(button)
    }
    document.body.append(controls)

    const maximize = controls.children[1]
    const updateMaximize = async () => {
      const maximized = await invoke('is_maximized')
      const label = maximized ? 'Restore' : 'Maximize'
      maximize.setAttribute('aria-label', label)
      maximize.title = label
      maximize.querySelector('path').setAttribute('d', maximized
        ? 'M6 5h6a1 1 0 0 1 1 1v6a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1z M3 10V4a1 1 0 0 1 1-1h6'
        : 'M3 3h10v10H3z')
    }
    window.addEventListener('resize', () => { void updateMaximize() })
    void updateMaximize()

    for (const edge of ['North', 'South', 'West', 'East', 'NorthWest', 'NorthEast', 'SouthWest', 'SouthEast']) {
      const handle = document.createElement('div')
      handle.className = 'dsh-window-resize'
      handle.dataset.edge = edge
      handle.setAttribute('aria-hidden', 'true')
      handle.addEventListener('pointerdown', event => {
        if (event.button === 0) void invoke('start_resize_dragging', { value: edge })
      })
      document.body.append(handle)
    }

    document.addEventListener('mousedown', event => {
      if (event.button !== 0 || !(event.target instanceof Element)) return
      const header = event.target.closest('[data-window-drag]')
      if (!header || event.target.closest('button, a, input, textarea, select, [role="button"], [contenteditable="true"]')) return
      if (event.detail === 2) void invoke('toggle_maximize')
      else if (event.detail === 1) void invoke('start_dragging')
    })
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install, { once: true })
  else install()
})()
