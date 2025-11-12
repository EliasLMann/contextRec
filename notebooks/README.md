# Notebook Index

This directory contains focused notebooks derived from `Lab2_multi_task_modal.ipynb`. Run them sequentially when you need the full pipeline, or open an individual notebook when you only need to inspect that specific stage.

1. **01_data_analysis.ipynb** – Loads the cleaned catalog, deduplicates tracks, and rolls raw genres into grouped categories with quick distribution plots.
2. **02_encoder_training.ipynb** – Builds the VAE encoder/decoder pair, handles feature preprocessing, and trains/saves the latent representation.
3. **03_encoder_inference.ipynb** – Explores the latent space, computes activity-specific centroids, and synthesizes user preference datasets.
4. **04_multitask_training.ipynb** – Trains the general classifier, initializes personalized heads, and performs the multi-task optimization loops.
5. **05_multitask_inference.ipynb** – Generates evaluation plots/metrics for each user head and runs the additional experiments/fine-tuning loops.

> Notebooks 03–05 expect that you have already run the earlier notebooks in order so that required artifacts (e.g., trained encoder weights, generated user datasets) are available in memory or on disk.
