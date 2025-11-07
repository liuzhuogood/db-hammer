async function loadConfig() {
    const response = await fetch('/api/config');
    const data = await response.json();
    const editor = document.getElementById('config-editor');
    editor.value = JSON.stringify(data, null, 2);
}

async function saveConfig() {
    const editor = document.getElementById('config-editor');
    try {
        const payload = JSON.parse(editor.value);
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const result = await response.json();
        showResult(JSON.stringify(result, null, 2));
    } catch (error) {
        showResult(`保存失败: ${error}`);
    }
}

async function testConnections() {
    try {
        const editor = document.getElementById('config-editor');
        const payload = JSON.parse(editor.value);
        const response = await fetch('/api/test-connections', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connections: payload.connections || [] }),
        });
        const result = await response.json();
        showResult(JSON.stringify(result, null, 2));
    } catch (error) {
        showResult(`测试失败: ${error}`);
    }
}

function formatConfig() {
    try {
        const editor = document.getElementById('config-editor');
        const payload = JSON.parse(editor.value);
        editor.value = JSON.stringify(payload, null, 2);
    } catch (error) {
        showResult(`格式化失败: ${error}`);
    }
}

function showResult(message) {
    const results = document.getElementById('results');
    results.textContent = message;
}

document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
    document.getElementById('format-button').addEventListener('click', formatConfig);
    document.getElementById('save-button').addEventListener('click', saveConfig);
    document.getElementById('test-button').addEventListener('click', testConnections);
});
