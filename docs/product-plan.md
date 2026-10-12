# Visione e piano di prodotto

## Obiettivo

Stratosphere permette di creare, sviluppare, modificare e migliorare strategie quantitative attraverso agenti, testarle su dati passati o in tempo reale e attivarle con trade reali. La gestione del portfolio coordina capitale e posizioni delle strategie selezionate.

Il ruolo nella suite Trading Sphere è trasformare idee di trading in strategie verificabili e gestibili nel tempo. Le interfacce con gli altri software della suite saranno definite separatamente; il funzionamento di Stratosphere non deve dipendere da integrazioni ancora inesistenti.

## Casi d'uso principali

| Caso | Risultato atteso |
| --- | --- |
| Importare un Reel, video, articolo o post | Regole estratte, riferimenti alle fonti, ambiguità visibili |
| Importare una strategia TradingView o GitHub | Analisi del materiale accessibile, dipendenze e semantica da verificare |
| Descrivere una strategia originale | Specifica completa e prima implementazione |
| Modificare una strategia | Nuova versione e confronto con la precedente |
| Richiedere miglioramenti | Varianti motivate, budget di ricerca e risultati separati per esperimento |
| Testare su storico | Report riproducibile con operazioni, costi e limiti del test |
| Testare con dati live | Segnali osservati o paper trading senza inviare ordini reali |
| Attivare più strategie | Allocazioni e posizioni coordinate entro un mandato esplicito |
| Gestire una strategia in produzione | Monitoraggio, pausa, disattivazione e aggiornamento controllato |

## Esperienza prevista

L'utente crea un progetto strategia, aggiunge fonti o una descrizione e vede cosa è stato ricostruito e cosa manca. Può correggere le regole prima dell'implementazione, lanciare esperimenti e confrontare versioni con lo stesso protocollo di test.

Ogni strategia presenta una scheda con origine, regole, versioni, risultati, stato operativo e motivi delle decisioni. La vista portfolio mostra capitale, allocazioni, posizioni aggregate, contributo delle strategie, limiti e ordini. Le spiegazioni degli agenti sono collegate alle evidenze disponibili.

Prima dell'attivazione reale, l'utente seleziona versione, conto, capitale, strumenti e limiti operativi. La piattaforma rende visibile la differenza tra risultati storici, risultati simulati live e risultati reali.

## Requisiti trasversali

- Tracciabilità dalla fonte alla regola, dalla regola al segnale e dal segnale all'ordine.
- Riproducibilità dei test attraverso versioni di strategia, dataset e configurazione.
- Separazione tra ricerca e runtime operativo: un esperimento non modifica una strategia attiva.
- Budget configurabili per chiamate ai modelli, durata dei job e numero di varianti.
- Gestione esplicita di dati incompleti, fonti inaccessibili e operazioni fallite.
- Credenziali esterne ai documenti di ricerca e agli input dei modelli.

## Confini iniziali

La visione include tutte le fonti elencate e il trading reale, ma il primo MVP si concentra sulla ricerca e sul backtest. Non sono ancora scelti asset class, broker, frequenze operative, interfaccia o modalità di hosting. Il supporto a trading ad alta frequenza, gestione di conti di terzi e marketplace di strategie non fa parte del piano iniziale.

Il successo iniziale si misura con un flusso completo e verificabile, la qualità delle regole ricostruite e la riproducibilità degli esperimenti. Un rendimento elevato su un singolo test non è un criterio sufficiente.
