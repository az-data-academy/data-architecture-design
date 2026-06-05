# Gestion des dépendances Jupyter

## Fichiers

| Fichier | Rôle | À modifier ? |
|---------|------|--------------|
| `requirements.in` | Contraintes souples (source of truth) | ✅ Oui |
| `requirements.txt` | Lockfile compilé par uv (249 packages) | ❌ Généré |

## Modifier les dépendances

```bash
# 1. Éditer requirements.in
nano infra/jupyter/requirements.in

# 2. Régénérer le lockfile (uv résout tous les conflits transitifs)
uv pip compile infra/jupyter/requirements.in \
  --python-version 3.11 \
  --output-file infra/jupyter/requirements.txt

# 3. Committer les deux fichiers
git add infra/jupyter/requirements.in infra/jupyter/requirements.txt
git commit -m "deps: mettre à jour les dépendances Jupyter"
```

## Installer uv

```bash
pip install uv
# ou
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Conflits connus résolus par uv

| Conflit | Résolution |
|---------|-----------|
| `feast[redis]` exige `redis<5` | `feast==0.49.0` + `redis==4.6.0` |
| `requests` version entre pyiceberg et feast | `requests==2.34.2` (compatible les deux) |
| `pynessie` versionnage ≠ serveur Nessie | `pynessie==0.67.0` (max PyPI, indépendant) |
