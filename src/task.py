class WorkerAPP:
    pass

app = WorkerAPP(queue=... ,another_param=...)


app.post("train_model.{model_id}")
def do_smth(body, model_id):
    pass

app.post("another_method")
def do_smth(body):
    pass


app.post("one.{more}.{method}")
def do_smth(body, more, method):
    pass
