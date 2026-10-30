# OuiCestFait — backend FastAPI de secours

Ce dépôt contient le backend FastAPI historique de OuiCestFait. Au 30/10/2026, il est maintenu et testé comme composant **legacy / optionnel** : aucun service Railway OuiCestFait actif n'est actuellement visible et le chemin métier principal constaté repose sur les scénarios Make de production et le classeur Google Sheets central.

## Documentation de référence

- [`PRODUCTION_STATE.md`](PRODUCTION_STATE.md) — source de vérité technique synthétique.
- [`DEPLOYMENT_CHECKLIST.md`](DEPLOYMENT_CHECKLIST.md) — procédure de changement, contrôles et rollback.
- [`SECURITY.md`](SECURITY.md) — règles de sécurité et gestion des secrets.

## Références de production

- MAIN Make : `OCF_MAIN_v115c_V8_4_1_PRODUCTION` — ID `9181656`
- Stripe Make : `OCF_STRIPE_WEBHOOK_v107_PRODUCTION` — ID `9456656`
- Numéro WhatsApp public : `07 59 05 57 81`
- Monitoring primaire : ChatGPT + GitHub + Google Sheets + email Gmail quotidien à 09:00
- Monitoring Make v102E : legacy/degraded, hors chemin critique

Ne pas recréer automatiquement un service Railway, ne pas reconnecter/réenregistrer le numéro WhatsApp/WABA et ne pas réactiver d'ancien scénario sans dépendance ou défaut réellement reproduit.

## Backend FastAPI

Endpoints principaux du composant de secours :

- `GET /health` — santé du backend
- `POST /mission/demande` — création/analyse d'une mission
- `POST /mission/validation` — validation d'une mission

Les paramètres sensibles doivent être fournis par variables d'environnement. Ne jamais committer de clés, tokens ou secrets.

## Tarification Province de référence

Règle métier validée :

`1 €/km + 15 € repas + (péage estimé × 2)`, puis `+30 %` de marge.

Le calcul du backend compare l'approche vers le point de départ depuis **Mormant 77720** et **Paris**, et retient l'approche la plus longue avant d'ajouter le trajet départ → arrivée.

## Qualité et sécurité

La CI `OCF Safety CI` vérifie notamment :

- compilation de `main.py` ;
- `/health` ;
- règle tarifaire Province ;
- anti-boucle ;
- double base Mormant/Paris ;
- masquage des tokens, secrets et identifiants Stripe dans les logs.

Toute modification technique doit passer par une branche et une pull request avec CI verte avant fusion dans `main`. La CI s'exécute aussi sur les pushes directs vers `main` afin de détecter une dérive même tant que la protection de branche GitHub n'est pas activée au niveau plateforme.

## Développement local

```powershell
.\run.ps1
```

Puis ouvrir `http://127.0.0.1:8000/docs`.

Le fichier `index.html` du dépôt n'est **pas** le site public OuiCestFait. Le site public reste `https://www.ouicestfait.fr`.
