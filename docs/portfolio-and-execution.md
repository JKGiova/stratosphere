# Portfolio, posizioni ed esecuzione reale

## Mandato di gestione

L'agente portfolio coordina le strategie attive: propone il capitale da assegnare, decide quali segnali accogliere e propone aperture, incrementi, riduzioni o chiusure in funzione dello stato complessivo. I limiti configurati dall'utente vengono applicati da controlli deterministici esterni al modello.

Il mandato comprende conto, strumenti ammessi, capitale, strategie/versioni attive, limiti per posizione e strategia, esposizione complessiva, leva ove prevista, concentrazione, perdita e drawdown massimi, frequenza di ribilanciamento e modalità di approvazione degli ordini. Le soglie e il metodo di allocazione sono decisioni ancora aperte.

## Dagli agenti alle posizioni

1. Le strategie producono segnali strutturati con versione, timestamp e scadenza.
2. Il sistema raccoglie capitale, posizioni, ordini aperti e stato dei feed.
3. L'agente portfolio propone allocazioni e variazioni delle esposizioni.
4. I controlli validano proposta, freschezza dello stato, disponibilità del capitale e limiti.
5. Il gateway traduce gli intenti approvati in ordini compatibili con il provider.
6. Fill e riconciliazione aggiornano il ledger; il portfolio viene rivalutato sullo stato effettivo.

Segnali di strategie diverse sullo stesso strumento non devono creare ordini indipendenti incompatibili. Definire una politica per netting, segnali opposti, attribuzione del capitale e priorità. Mantenere posizioni virtuali per strategia e posizioni aggregate del conto, con attribuzione dei fill e del P&L.

## Aperture e chiusure

Le uscite possono essere richieste dalla logica della strategia, dal ribilanciamento o da un limite di rischio. Il sistema identifica l'origine della decisione e applica le precedenze previste dal mandato. L'agente non può annullare un vincolo di rischio o ampliare autonomamente il proprio mandato.

Le protezioni operative essenziali devono funzionare anche senza modello disponibile. Dove il provider lo consente, valutare ordini protettivi lato broker; dove non lo consente, dichiarare la dipendenza dal runtime e verificare la relativa procedura di emergenza.

## Requisiti dell'esecuzione

- Identificatori idempotenti e deduplicazione per evitare ordini ripetuti dopo retry.
- Gestione di ordini accettati, rifiutati, parzialmente eseguiti, cancellati e scaduti.
- Validazione di quantità, tick size, minimi, saldo/margine e stato del mercato.
- Riconciliazione con il provider dopo timeout, disconnessioni e riavvii.
- Stato ambiguo di un ordine trattato come da riconciliare, senza reinvio automatico alla cieca.
- Tracciamento di slippage, costi, fill e differenze dal target.
- Separazione delle credenziali paper e live e accessi limitati alle operazioni necessarie.

## Attivazione e arresto

Il trading reale è disabilitato per impostazione iniziale. L'attivazione identifica esplicitamente versione, conto e mandato. È un requisito della futura piattaforma; la stesura di questi documenti non configura conti né abilita operazioni.

Prevedere almeno tre azioni distinte: **bloccare nuovi ingressi**, **cancellare gli ordini pendenti** e **richiedere la chiusura delle posizioni**. La richiesta di chiusura non garantisce un fill immediato: monitorare l'esposizione residua e notificare eventuali fallimenti.

Dati obsoleti, errori di riconciliazione, violazioni dei limiti e anomalie del provider bloccano nuove aperture secondo politiche definite in anticipo. Stabilire separatamente come gestire le posizioni esistenti. Una nuova versione non sostituisce quella attiva senza verificare anche ordini pendenti e piano di transizione.

## Osservabilità

La vista operativa mostra capitale, esposizioni, posizioni, ordini, P&L realizzato/non realizzato, contributi per strategia, stato del modello e dei servizi, limiti utilizzati e motivi di blocco. Ogni ordine è collegato a segnali, proposta portfolio, controlli e fill. Gli allarmi includono un'azione operativa chiara e un registro verificabile.
