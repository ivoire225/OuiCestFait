# OuiCestFait — checklist déploiement / rollback

Cette checklist s'applique à toute modification du backend ou d'un composant technique lié au parcours de production.

## Avant modification

- [ ] Le défaut ou besoin est réellement reproduit et documenté.
- [ ] Vérifier `PRODUCTION_STATE.md` et ne pas suivre une ancienne version par erreur.
- [ ] Créer une branche `maintenance/*` depuis `main`.
- [ ] Ne pas modifier le WABA, le numéro WhatsApp, MAIN ou Stripe sans nécessité démontrée.
- [ ] Ne pas introduire de secret dans GitHub, les logs ou la documentation.
- [ ] Prévoir le rollback avant de toucher à la production.

## Contrôles automatiques obligatoires

- [ ] `python -m py_compile main.py` passe.
- [ ] Installation de `requirements.txt` passe.
- [ ] `tests/test_main_safety.py` passe.
- [ ] `/health` est présent et testable.
- [ ] Règle Province : 1 EUR/km + 15 EUR + péage x2, puis +30 %.
- [ ] Double base Mormant / Paris conservée.
- [ ] Anti-boucle conservé.
- [ ] Masquage des secrets dans les logs conservé.
- [ ] Aucun changement involontaire du numéro WhatsApp ou du WABA.

## Avant fusion

- [ ] Ouvrir une pull request vers `main`.
- [ ] Attendre `OCF Safety CI = success`.
- [ ] Relire le diff complet.
- [ ] Vérifier qu'aucune règle métier non validée n'a été introduite.
- [ ] Fusionner seulement si les contrôles sont verts.

## Après fusion

- [ ] Vérifier que la CI sur `main` reste verte.
- [ ] Si un service actif est relié à GitHub, vérifier son déploiement et `/health`.
- [ ] Vérifier que les scénarios Make de production continuent de fonctionner.
- [ ] Contrôler le classeur central : aucune nouvelle erreur / mission bloquée.
- [ ] Contrôler l'email quotidien de monitoring.

## Rollback

En cas de défaut :

1. ne pas corriger directement dans la production sous pression ;
2. identifier le dernier commit connu sain ;
3. restaurer `main` vers cette révision ou effectuer un revert explicite ;
4. vérifier la santé du service et du parcours métier ;
5. conserver le commit défaillant dans l'historique pour audit ;
6. documenter la cause avant une nouvelle tentative.

Référence historique saine avant la vague de durcissement du 30/10/2026 : `010998c46d537fd441d5f708516f0fb36a166c2d`.
