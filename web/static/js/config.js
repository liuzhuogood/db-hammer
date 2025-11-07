(function () {
  async function loadConfig() {
    const response = await fetch('/api/config');
    if (!response.ok) {
      throw new Error('Failed to load config');
    }
    const data = await response.json();
    document.getElementById('config-path').value = data.metadata?.config_path || 'Loaded from manager';
    window.currentConfig = data;
  }

  async function saveConfig() {
    if (!window.currentConfig) {
      return;
    }
    const response = await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(window.currentConfig),
    });
    if (!response.ok) {
      const err = await response.json();
      alert(err.error || 'Failed to save configuration');
      return;
    }
    alert('Configuration saved');
  }

  async function loadConnections() {
    const response = await fetch('/api/connections');
    if (!response.ok) {
      document.getElementById('connections').textContent = 'Unable to load connections';
      return;
    }
    const data = await response.json();
    document.getElementById('connections').textContent = JSON.stringify(data, null, 2);
  }

  document.getElementById('load-config').addEventListener('click', async () => {
    try {
      await loadConfig();
      await loadConnections();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById('save-config').addEventListener('click', async () => {
    await saveConfig();
  });

  // auto load on init
  loadConfig().then(loadConnections).catch(() => {
    document.getElementById('connections').textContent = 'Load configuration to view connections.';
  });
})();
