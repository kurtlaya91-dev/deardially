// ADDED: This handles creating/removing shortcuts on Windows during install/uninstall
if (require('electron-squirrel-startup')) return; 

const { app, BrowserWindow } = require('electron');
const path = require('path');

function createWindow() {
  const win = new BrowserWindow({
    // 10% larger than 1440x900
    width: 1584,
    height: 990,
    resizable: false, 
    show: false, // Prevents the white flash
    backgroundColor: '#f9fafb', // Matches your UI background
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });

  win.loadFile('index.html');

  // Only show the window once the content is fully rendered
  win.once('ready-to-show', () => {
    win.show();
  });
}

app.whenReady().then(createWindow);

// Standard macOS window behavior
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});