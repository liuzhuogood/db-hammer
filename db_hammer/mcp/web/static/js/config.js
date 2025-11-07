document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('config-form');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const payload = {
      host: formData.get('host'),
      port: Number(formData.get('port')),
      storage_path: formData.get('storage_path'),
      log_level: formData.get('log_level'),
      security: {
        enable_token_auth: formData.get('security.enable_token_auth') === 'on',
        token_secret: formData.get('security.token_secret') || null,
      },
      databases: [],
    };
    const response = await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (response.ok) {
      alert('配置已保存');
    } else {
      alert('保存失败');
    }
  });
});
