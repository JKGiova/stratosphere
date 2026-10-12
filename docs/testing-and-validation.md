# Dati, test e validazione

## Modalità di prova

| Modalità | Dati | Ordini | Obiettivo |
| --- | --- | --- | --- |
| Backtest | Storico versionato | Simulati | Ricostruire il comportamento passato |
| Replay | Eventi storici riprodotti in ordine | Simulati | Verificare il runtime e i casi operativi |
| Forward test | Feed in tempo reale | Nessuno | Osservare segnali, ritardi e disponibilità dati |
| Paper trading | Feed in tempo reale | Simulati o ambiente paper del provider | Valutare portfolio ed esecuzione simulata |
| Live | Feed in tempo reale | Reali | Operare entro il mandato attivato |

La simulazione deve rendere visibili le proprie assunzioni: paper e backtest non riproducono necessariamente liquidità, priorità degli ordini e fill reali.

## Contratto dei dati

Registrare provider, strumenti, intervallo, timezone, frequenza, timestamp dell'evento e di disponibilità, trasformazioni, hash o snapshot immutabile. Verificare duplicati, ordinamento, buchi, barre incomplete e cambi di simbolo. Le strategie usano soltanto informazioni disponibili al momento del segnale.

Considerare sessioni e corporate action per gli asset che le richiedono; includere funding, prestito titoli, scadenze o rollover quando applicabili. Un dataset non adatto al mercato scelto blocca la validazione o limita esplicitamente il significato del report.

## Protocollo storico

1. Fissare ipotesi, benchmark, metriche, costi e criteri prima degli esperimenti.
2. Separare temporalmente dati di sviluppo, validazione e campione finale fuori campione.
3. Applicare warm-up e regole per evitare contaminazione tra finestre; usare walk-forward quando appropriato.
4. Simulare commissioni, spread, slippage, latenza e vincoli di esecuzione rilevanti.
5. Confrontare la strategia con baseline semplici e versioni precedenti sullo stesso protocollo.
6. Verificare sensibilità ai parametri, regimi, periodi e peggioramento dei costi.
7. Conservare configurazione, codice, dataset, operazioni e risultati di ogni tentativo.

Il campione finale non è uno strumento di ottimizzazione. Se viene usato per scegliere nuove varianti, non è più una verifica indipendente e serve un nuovo protocollo. Tenere conto del numero di tentativi quando si interpreta la variante selezionata.

## Errori da verificare

- Look-ahead: uso di prezzi o indicatori non ancora disponibili.
- Survivorship bias e selezione opportunistica degli strumenti o dei periodi.
- Repainting, specialmente nelle conversioni da TradingView.
- Differenze tra segnale a chiusura e riempimento allo stesso prezzo.
- Stop e take profit toccati nella stessa barra senza dati per ricostruirne l'ordine.
- Costi omessi, liquidità irrealistica e operazioni superiori alla capacità del mercato.
- Overfitting dei parametri e selezione ripetuta sul fuori campione.

Quando lo storico non consente di determinare un fill, adottare una convenzione dichiarata e conservativa oppure richiedere dati più granulari.

## Report minimo

Ogni report contiene versione, fonti, ipotesi, dataset, finestre, modello dei costi, baseline, rendimento lordo/netto, curva del capitale, drawdown, numero di trade, turnover, esposizione e distribuzione dei risultati. Riporta metriche di rendimento aggiustato per il rischio solo con convenzioni e frequenza esplicite; segnala campioni insufficienti e concentrazione del risultato in pochi trade.

Includere operazioni consultabili, confronto tra sviluppo e fuori campione, stress test, limiti e motivazione della proposta del validatore. Salvare i valori calcolati dal motore: il modello li commenta senza ricostruirli dal testo.

## Criteri di promozione

Le soglie numeriche dipendono da mercato, strategia e mandato e restano da definire prima dei test. La promozione richiede comunque correttezza delle regole, dati adeguati, risultati riproducibili al netto dei costi, evidenze fuori campione, robustezza e assenza di criticità irrisolte.

Per il passaggio paper → live, definire in anticipo durata o numero minimo di osservazioni, tolleranze tra comportamento atteso e osservato, requisiti di latenza e affidabilità. Verificare anche riconciliazione, limiti, interruzioni, restart e procedura di arresto. Non promuovere automaticamente una strategia per il solo rendimento.
