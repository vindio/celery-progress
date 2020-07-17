var CeleryWebSocketMultiTaskProgressBar = (function () {

    function onSuccessDefault(progressBarElement, progressBarMessageElement, result) {
        CeleryProgressBar.onSuccessDefault(progressBarElement, progressBarMessageElement);
    }

    function onResultDefault(resultElement, result) {
        CeleryProgressBar.onResultDefault(resultElement, result);
    }

    function onErrorDefault(excMessage, progressBarElement, progressBarMessageElement) {
        CeleryProgressBar.onErrorDefault(progressBarElement, progressBarMessageElement, excMessage);
    }

    function onProgressDefault(progressBarElement, progressBarMessageElement, progress) {
        progressBarElement.style.backgroundColor = '#68a9ef';
        progressBarElement.style.width = progress.percent + "%";
        var description = progress.description || "";
        progressBarMessageElement.innerHTML = progress.current + ' de ' + progress.total + ' procesados. ' + description;
    }

    var tasks_options = {};
    var task_ids = [];
    
    function initProgress (progressUrl, tasks) {
        for (i=0;i<tasks.length; i++) {
            let task_id = tasks[i].task_id
            let options = tasks[i].options || {};
            let task_options = {};
            task_options.progressBarId = options.progressBarId || 'progress-bar-' + task_id;
            task_options.progressBarMessageId = options.progressBarMessageId || 'progress-bar-message-' + task_id;
            task_options.progressBarElement = options.progressBarElement || document.getElementById(task_options.progressBarId);
            task_options.progressBarMessageElement = options.progressBarMessageElement || document.getElementById(task_options.progressBarMessageId);
            task_options.onProgress = options.onProgress || onProgressDefault;
            task_options.onSuccess = options.onSuccess || onSuccessDefault;
            task_options.onError = options.onError || onErrorDefault;
            task_options.resultElementId = options.resultElementId || 'celery-result-' + task_id;
            task_options.resultElement = options.resultElement || document.getElementById(task_options.resultElementId);
            task_options.onResult = options.onResult || onResultDefault;
            tasks_options[task_id] = task_options;
            task_ids.push(task_id);
        }

        var ProgressSocket = new WebSocket(progressUrl);

        ProgressSocket.onopen = function (event) {
            ProgressSocket.send(JSON.stringify({'type': 'follow_tasks', 'task_ids': task_ids}));
            for (i=0;i<task_ids.length;i++) {
                ProgressSocket.send(JSON.stringify({'type': 'check_task_completion', 'task_id': task_ids[i]}));
            }
        };

        ProgressSocket.onmessage = function (event) {
            var data = JSON.parse(event.data);
            let task_id = data.task_id || data.id;
            if (task_id == null) {
                console.error("Error: no se pudo obtener task_id del mensaje", event.data);
                return;
            }
            if (data.progress) {
                options = tasks_options[task_id]
                progressBarElement = options.progressBarElement;
                progressBarMessageElement = options.progressBarMessageElement;
                if (data.progress.progress_id) {
                    progress_id = data.progress.progress_id;
                    element_id = options.progressBarId + "-" + progress_id;
                    element = document.getElementById(element_id);
                    if (element) {
                        progressBarElement = element;
                    }
                    element_id = options.progressBarMessageId + "-" + progress_id;
                    element = document.getElementById(element_id);
                    if (element) {
                        progressBarMessageElement = element;
                    }
                }
                tasks_options[task_id].onProgress(
                    progressBarElement,
                    progressBarMessageElement,
                    data.progress
                );
            }
            if (data.complete) {
                if (data.success) {
                    tasks_options[task_id].onSuccess(
                        tasks_options[task_id].progressBarElement,
                        tasks_options[task_id].progressBarMessageElement,
                        data.result
                    );
                } else {
                    tasks_options[task_id].onError(
                        tasks_options[task_id].progressBarElement,
                        tasks_options[task_id].progressBarMessageElement,
                        data.result
                    );
                }
                if (data.result) {
                    tasks_options[task_id].onResult(
                        tasks_options[task_id].resultElement,
                        data.result
                    );
                }
                ProgressSocket.close();
            }
        }
    }
    return {
        onSuccessDefault: onSuccessDefault,
        onResultDefault: onResultDefault,
        onErrorDefault: onErrorDefault,
        onProgressDefault: onProgressDefault,
        initProgressBar: initProgress,
    };
})();
