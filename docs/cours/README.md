# Contenu théorique de la formation

Ce dossier contient le support de cours complet accompagnant les labs pratiques, jour par jour.

| Fichier | Contenu |
|---|---|
| `J1_iceberg_polaris.md` / `.docx` | Fondamentaux Apache Iceberg (snapshots, manifests, CoW/MoR), catalogue Polaris |
| `J2_nessie_mlops.md` / `.docx` | Versioning Git-like avec Nessie, CI/CD pour la donnée, Feature Store (Feast), MLOps (MLflow) |
| `J3_gouvernance_lineage.md` / `.docx` | Gouvernance, lineage, data contracts, conformité RGPD/UEMOA |

Les fichiers `.md` sont la source de référence (versionnable, diffable). Les `.docx` sont générés depuis le Markdown via pandoc et fournis pour une lecture hors-ligne ou une diffusion en environnement bureautique classique.

Pour régénérer les `.docx` après une modification des `.md` :

```bash
cd docs/cours
for f in J1_iceberg_polaris J2_nessie_mlops J3_gouvernance_lineage; do
  pandoc "${f}.md" -o "${f}.docx" --toc --toc-depth=2
done
```

Ce contenu théorique est conçu pour être lu **avant** ou **en parallèle** des labs correspondants — il explique le « pourquoi » derrière chaque manipulation technique, et documente plusieurs pièges réels rencontrés lors de la validation de cette formation (apostrophes en SQL, chemins API Nessie v1 vs v2, namespaces Polaris, choix CoW/MoR en contexte réglementaire).
