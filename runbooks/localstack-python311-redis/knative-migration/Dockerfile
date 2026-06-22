FROM public.ecr.aws/sam/build-python3.11:latest

ENV LANG=en_US.UTF-8
ENV PATH=/var/lang/bin:/usr/local/bin:/usr/bin:/bin:/opt/bin
ENV PYTHONPATH=/var/task
ENV LD_LIBRARY_PATH=/var/lang/lib:/usr/lib64:/lib64:/var/task:/var/task/lib
ENV LAMBDA_TASK_ROOT=/var/task
ENV LAMBDA_RUNTIME_DIR=/var/runtime

WORKDIR /var/task

COPY build/bundle/fmi.so /var/task/fmi.so
COPY build/bundle/lib/ /var/task/lib/
COPY lambda_function.py /var/task/lambda_function.py
COPY worker_core.py /var/task/worker_core.py
COPY vm_worker.py /var/task/vm_worker.py
COPY fmi-worker.json /var/task/fmi-worker.json
COPY knative-migration/fmi-orchestrator.json /var/task/fmi-orchestrator.json
COPY knative-migration/knative_http.py /var/task/knative_http.py
COPY knative-migration/orchestrator.py /var/task/orchestrator.py

RUN python3.11 -c "import fmi; assert hasattr(fmi, 'FTControlPlane'); print('fmi import smoke test ok')"

EXPOSE 8080
ENTRYPOINT []
CMD ["python3.11", "-u", "/var/task/knative_http.py"]
