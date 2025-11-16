# 🩻 CXR-CDS: Chest X-ray Clinical Decision Support System

An **OOP-based FastAPI service** that interprets chest X-rays using a modular vision model
and a Large Language Model (LLM) for diagnostic reasoning.  
Now supports **free-text queries** such as:

```json
{
  "xray_id": "CXR_000123",
  "query": "Diagnose the X-ray"
}



cxr_cds/
├── app/
│   ├── __init__.py
│   ├── main.py                   # FastAPI entrypoint
│   ├── config.py                 # environment + constants
│   ├── logger.py                 # rotating logging setup
│   ├── schemas.py                # request/response models
│   ├── utils.py                  # helpers (image loader, custom errors)
│   ├── models/
│   │   ├── __init__.py
│   │   ├── vision.py             # Vision model (dummy / pytorch)
│   │   └── llm_client.py         # LLM client (dummy / OpenAI)
│   ├── services/
│   │   ├── __init__.py
│   │   └── analyzer.py           # Orchestrator (vision + llm)
│   └── tests/                    # (optional) tests
├── images/                       # store chest X-ray images here
│   ├── CXR_000123.png
│   └── ...
├── logs/
│   └── app.log                   # auto-created
├── .env.sample
├── requirements.txt
├── README.md
└── run.sh
