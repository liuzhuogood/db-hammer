(function () {
    const editor = document.getElementById('config-editor');
    const refreshBtn = document.getElementById('refresh-config');
    const saveBtn = document.getElementById('save-config');
    const statusView = document.getElementById('config-status');
    const testTextarea = document.getElementById('test-payload');
    const testStatus = document.getElementById('test-status');
    const testBtn = document.getElementById('run-test');

    let apiKey = window.localStorage.getItem('dbHammerApiKey') || '';

    function ensureKey() {
        if (!apiKey) {
            apiKey = window.prompt('请输入API Key');
            if (apiKey) {
                window.localStorage.setItem('dbHammerApiKey', apiKey);
            }
        }
        return apiKey;
    }

    function headers() {
        const key = ensureKey();
        const base = {'Content-Type': 'application/json'};
        if (key) {
            base['X-API-Key'] = key;
        }
        return base;
    }

    function setStatus(element, message, type) {
        element.textContent = message;
        element.classList.remove('success', 'error');
        if (type) {
            element.classList.add(type);
        }
    }

    async function fetchConfig() {
        try {
            const response = await fetch('/api/config', {headers: headers()});
            if (response.status === 401) {
                apiKey = '';
                ensureKey();
                return fetchConfig();
            }
            const data = await response.json();
            editor.value = JSON.stringify(data, null, 2);
            setStatus(statusView, '配置已加载', 'success');
        } catch (error) {
            setStatus(statusView, '加载配置失败: ' + error, 'error');
        }
    }

    async function saveConfig() {
        try {
            const payload = JSON.parse(editor.value || '{}');
            const response = await fetch('/api/config', {
                method: 'POST',
                headers: headers(),
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (response.ok) {
                setStatus(statusView, '配置已保存', 'success');
                editor.value = JSON.stringify(data, null, 2);
            } else {
                setStatus(statusView, data.message || '保存失败', 'error');
            }
        } catch (error) {
            setStatus(statusView, '保存失败: ' + error, 'error');
        }
    }

    async function runTest() {
        try {
            const payload = JSON.parse(testTextarea.value || '{}');
            const response = await fetch('/api/config/test', {
                method: 'POST',
                headers: headers(),
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (response.ok) {
                setStatus(testStatus, '测试成功', 'success');
            } else {
                setStatus(testStatus, data.message || '测试失败', 'error');
            }
        } catch (error) {
            setStatus(testStatus, '测试失败: ' + error, 'error');
        }
    }

    if (refreshBtn) {
        refreshBtn.addEventListener('click', fetchConfig);
    }
    if (saveBtn) {
        saveBtn.addEventListener('click', saveConfig);
    }
    if (testBtn) {
        testBtn.addEventListener('click', runTest);
    }

    fetchConfig();
})();
