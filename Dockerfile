FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir numpy pandas openpyxl streamlit altair && \
    pip install --no-cache-dir -e .

EXPOSE 7860

CMD ["streamlit", "run", "tools/run_ui.py", \
     "--server.port=7860", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
