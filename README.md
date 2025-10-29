# Introduction
[![Author](https://img.shields.io/badge/Pattawee.P-blue?label=Author)](https://bodysbobb.github.io/) ![Last Updated](https://img.shields.io/github/last-commit/Bodysbobb/GTAPViz?label=Last%20Updated&color=blue)
![License](https://img.shields.io/github/license/Bodysbobb/trade-analysis?color=3CB371)

# Trade Analysis Automation
A **Python-based automation tool** for downloading, merging, and summarizing international trade and macroeconomic data using official APIs from **UN Comtrade**, **World Bank**, and **CEPII**.

🔗 **Full documentation and tutorial:** [Pattawee - Trade Research API](https://www.pattawee-pp.com/blog/2025/trade-research-api)

---

## 🌍 Overview
This repository provides the **complete source code, mapping files, and data templates** used in the automated trade analysis workflow described on my website.  
It includes:
- Python scripts to fetch and merge data from **UN Comtrade**, **World Bank**, and **CEPII**.  
- A pre-formatted Excel mapping file for indicator and ISO code alignment.  
- Prepared CEPII gravity dataset in symmetric format.  
- Example outputs (`.csv`, `.xlsx`) ready for econometric or gravity model research.

Use this repo to replicate or extend the workflow discussed in my [tutorial post](https://www.pattawee-pp.com/blog/2025/trade-research-api/).

**Main data sources:**
- [UN Comtrade API](https://comtradedeveloper.un.org/)
- [World Bank API](https://data.worldbank.org/)
- [CEPII Gravity Dataset](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=8)

---

## ⚙️ Key Features
- Unified configuration for **multi-source data collection**
- Supports **batch “all countries”** downloads without exceeding API limits  
- Automatically merges trade, macroeconomic, and gravity data  
- Generates **summary Excel reports** with coverage stats and missing data analysis  
- Consistent ISO3 country definitions across datasets  

---

## 🙌 Acknowledgment
This project integrates open data from:
- **UN Comtrade API (`comtradeapicall`)**
- **World Bank API (WDI)**
- **CEPII Gravity Dataset**

For additional explanations, setup details, and visual walkthroughs:  
👉 [pattawee-pp.com/blog/2025/trade-research-api](https://www.pattawee-pp.com/blog/2025/trade-research-api)

---

## 👨‍💻 Author
**Pattawee Puangchit**  
Ph.D. Candidate in Agricultural Economics, Purdue University  
Graduate Research Assistant at GTAP  
Research focus: *Trade policy, tariffs, NTMs, and CGE modeling*  

🌐 [Personal Website](https://www.pattawee-pp.com)  
📦 [GitHub Profile](https://github.com/bodysbobb)
