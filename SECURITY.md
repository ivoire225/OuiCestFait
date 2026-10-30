# Sécurité OuiCestFait

## Principes

- Aucun secret ne doit être stocké dans le dépôt GitHub.
- Les clés et tokens doivent être injectés uniquement par les gestionnaires de variables d'environnement des plateformes concernées.
- Ne jamais recopier dans les issues, PR, logs publics ou captures : clés API, secrets webhook, tokens d'accès, mots de passe ou cookies de session.
- Les identifiants Stripe Checkout et PaymentIntent ne sont pas des clés secrètes, mais doivent être tronqués dans les nouveaux logs pour réduire l'exposition inutile.

## Journalisation

Le backend masque ou tronque automatiquement :

- clés Stripe `sk_live_*` / `sk_test_*` ;
- secrets webhook `whsec_*` ;
- tokens Bearer ;
- champs `authorization`, `token`, `api_key`, `secret`, `password`, `x-ocf-token`, `debug_token`, etc. ;
- identifiants Checkout `cs_live_*` / `cs_test_*`.

Ne pas désactiver cette protection.

## WhatsApp / Meta

- Ne jamais réenregistrer, migrer ou recréer le numéro de production ou le WABA pour corriger un incident secondaire sans plan de retour arrière.
- Toute modification de connexion doit être testée sur une copie ou un environnement isolé lorsque cela est possible.

## Stripe

- Conserver la déduplication des événements webhook.
- Vérifier la signature du webhook avant passage en exploitation commerciale large.
- Les secrets Stripe doivent être tournés uniquement avec procédure de rollback et validation du nouveau webhook.

## Google Sheets

Le classeur central contient des données opérationnelles et doit rester privé. Au 30/10/2026, il n'est pas partagé publiquement.

## GitHub

- Utiliser une branche `maintenance/*`.
- Ouvrir une pull request vers `main`.
- Exiger une CI verte avant fusion.
- La branche `main` n'étant pas protégée au niveau plateforme au 30/10/2026, la CI s'exécute aussi sur les pushes directs afin de détecter immédiatement une dérive. La protection de branche reste recommandée dès que l'accès administrateur permet de l'activer.

## Incident

En cas de fuite suspectée :

1. ne pas publier la valeur du secret dans le ticket d'incident ;
2. révoquer / faire tourner le secret concerné ;
3. rechercher son exposition dans GitHub, logs, Drive et outils d'automatisation ;
4. vérifier les usages non autorisés ;
5. documenter la cause et la mesure corrective ;
6. vérifier les obligations de notification applicables si des données personnelles sont concernées.
