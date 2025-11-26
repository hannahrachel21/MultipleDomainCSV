# Multi-Domain CSV Generator

A unified, extensible data-simulation engine for generating domain-specific datasets, storing them in Google Sheets, and seeding them into a SQLite database.

## Overview

This project is a multi-domain synthetic dataset generator designed to simulate realistic business data across three domains:
- Retail
- Manufacturing
- Education

Each domain generates:
-Unique entities (products, equipment, students, etc.)
-Transactional data (sales, downtimes, progress, etc.)
-Mapped memory tables to maintain relationships
-Realistic timestamps, names, cycles, pricing, and behavior

The project supports:
-Fetching initial data from CSV files
-Auto-creating Google Sheets tabs if empty
-Seeding SQLite tables on first run
-Incrementally generating new records to populate datasets

This makes it suitable for:
-Data engineering practice
-ML model prototyping
-Dashboards & analytics

## Features
-Multi-domain simulation engine
Each domain contains:
1. Entity creation (Products, Stores, Equipment, Students…)
2. Randomized but realistic field generation
3. Intelligent ID generation
4. Probability-based creation of new entities

-CSV → Google Sheets → SQLite pipeline
1. Loads initial CSV seeds
2. Creates Sheets if empty
3. Inserts rows into SQLite tables
4. Maintains memory-mapping tables (e.g., sale → inventory)
5. Populates rows onto Google Sheets

## Supported Domains & Entities
1. Retail - Product, Store, Sale, Inventory
2. Manufacturing - Equipment, Technician, Downtime, Maintainence
3. Education - Student, Module, Learning Progress, Resource Usage

## Running the Simulator
1. Install dependencies
   pip install -r requirements.txt
3. Generate a credentials.json file for the Google Sheets API from the Google Cloud Console
4. Create YAML config files for each domain
5. Run the main script
   python main.py --domain "domain_name" --config "YAML_config_file_path"
