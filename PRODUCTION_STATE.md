# OuiCestFait — état de production de référence

Dernière mise à jour : 30/10/2026

Ce fichier est la source de vérité technique synthétique du projet. En cas de contradiction avec une ancienne note, un ancien blueprint ou une ancienne feuille de configuration, utiliser ce fichier puis vérifier le classeur Google Sheets central.

## Architecture active

- Parcours client principal : WhatsApp / scénarios Make de production.
- MAIN Make : `OCF_MAIN_v115c_V8_4_1_PRODUCTION` — ID `9181656`.
- Stripe Make : `OCF_STRIPE_WEBHOOK_v107_PRODUCTION` — ID `9456656`.
- Numéro WhatsApp public : `07 59 05 57 81`.
- Monitoring primaire : ChatGPT + GitHub + Google Sheets + email Gmail quotidien vers 09:00 Europe/Paris.
- Monitoring Make v102E ID `9499034` : `LEGACY_DEGRADED_NONCRITICAL`, hors chemin critique.
- Backend FastAPI de ce dépôt : composant de secours / legacy testé. Aucun service Railway OuiCestFait actif n'est actuellement visible via le connecteur Railway.

## Dispatch hybride

- Stratégie : `PROVIDER_FIRST_HYBRID`.
- Prestataires externes : chemin automatique seulement lorsqu'un vrai prestataire exploitable est actif.
- `P001 TEST PRESTATAIRE` : `paused / no` — interdit en production réelle.
- `P002 Prestataire test Combs` : `paused / no`.
- `OCF_SELF` : autorisé comme capacité interne, mais `disponible = no` par défaut et affectation uniquement sur décision administrative explicite.
- Transition OCF_SELF : `TAKE -> Assigné -> START -> En cours -> COMPLETE -> Livré` avec contrôles d'état et anti-double-affectation.

## Tarification

### Province — règle métier validée

`1 EUR/km + 15 EUR repas + (péage estimé x2)`, puis `+30 %` de marge.

Suppléments validés :
- aller/retour : `+50 %` ;
- nuit : `+30 %` ;
- week-end : `+20 %` ;
- volumineux : `+20 à +60 EUR` ;
- urgence : multiplicateur `x1,15` dans le backend de secours.

Le backend compare l'approche vers le point de départ depuis `Mormant 77720` et `Paris 75000` et retient l'approche la plus longue avant d'ajouter le trajet départ -> arrivée.

### Île-de-France

Le profil actuellement codé dans le backend de secours reste une référence technique et non un GO commercial définitif. Toute modification commerciale de la grille IDF doit être validée avant changement de production.

## Sécurité

- Les nouveaux logs du backend masquent tokens, secrets, clés Stripe, secrets webhook et tronquent les identifiants Checkout Stripe.
- Le classeur Google Sheets central n'est pas partagé publiquement.
- Les identifiants Stripe historiques complets sont conservés dans l'audit car ce ne sont pas des clés secrètes ; ne pas les recopier dans des canaux publics.
- Ne jamais committer de clé API, token, secret Stripe, secret webhook ou mot de passe.
- Toute modification technique doit passer par une branche `maintenance/*`, une PR et une CI verte.

## Éléments à ne pas modifier automatiquement

- WABA et numéro WhatsApp de production.
- MAIN `9181656` et Stripe `9456656` sans défaut reproduit et plan de rollback.
- Ancien monitoring Make pour tenter de corriger le `[400]` tant qu'il reste hors chemin critique.
- Blueprints historiques ou scénarios anciens simplement parce qu'ils existent.

## Blocages externes restant avant ouverture commerciale large

- qualification juridique définitive du modèle transport / conciergerie / intermédiaire ;
- formalités transport si applicables ;
- assurance(s) adaptée(s) ;
- identité juridique définitive et coordonnées professionnelles ;
- mentions légales / CGV / politique de confidentialité finalisées avec ces informations ;
- au moins un mode d'exécution réel validé : OCF_SELF juridiquement autorisé et/ou vrai prestataire contractuel ;
- pilote réel contrôlé après GO juridique.
