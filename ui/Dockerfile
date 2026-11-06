FROM python:3.12-slim
RUN pip install --no-cache-dir streamlit==1.50.* "psycopg[binary]==3.2.*" pandas altair
COPY app.py /app/app.py
EXPOSE 8501
CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--server.headless=true", "--browser.gatherUsageStats=false"]
