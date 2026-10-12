# Roadmap e decisioni aperte

## Fasi proposte

Le fasi indicano dipendenze e risultati verificabili, non date di consegna. Tutte sono attualmente **da implementare**; è stata creata soltanto la documentazione iniziale.

| Fase | Ambito | Criterio di completamento |
| --- | --- | --- |
| 0 — Fondazioni | Scegliere primo mercato, dati, stack e contratti | Decisioni motivate, specifica di esempio e protocollo di test definiti |
| 1 — MVP ricerca | Testo/articolo → estrazione → specifica → codice → backtest → report | Un caso completo riproducibile, con evidenze delle regole, costi e log |
| 2 — Ricerca iterativa | Versioni, modifiche, varianti e validazione fuori campione | Confronti riproducibili e tracciamento di tutti i tentativi entro un budget |
| 3 — Fonti esterne | YouTube, Instagram, X, TradingView e GitHub | Connettori verificati singolarmente, provenienza preservata e limiti di accesso espliciti |
| 4 — Dati live e paper | Feed, segnali, portfolio simulato e primo adattatore paper | Esecuzione prolungata con riconciliazione, restart e gestione delle anomalie |
| 5 — Trading reale | Primo provider live, mandato, controlli e gestione delle posizioni | Attivazione verificabile e prove di idempotenza, rischio e arresto completate |
| 6 — Evoluzione | Più strategie/provider, monitoraggio della deriva e miglioramenti continui | Portfolio aggregato coerente e promozione controllata delle nuove versioni |

## MVP: perimetro concreto

Supportare una fonte testuale o un articolo accessibile, un mercato, un formato di dati storico e un motore di backtest. Consentire revisione della specifica prima della generazione, esecuzione isolata e report con operazioni e configurazione completa.

Il primo esempio deve essere semplice e scelto per verificare le regole: condizioni d'ingresso e uscita esplicite, parametri limitati e casi attesi controllabili. Le verifiche comprendono corrispondenza specifica/codice, timing dei segnali, calcolo dei costi, gestione dei buchi dati e ripetibilità del risultato. Il criterio non richiede che l'esempio sia redditizio.

## Struttura futura del codice

Proposta da adattare dopo la scelta dello stack. Oggi esistono soltanto README e docs.

| Area logica proposta | Responsabilità |
| --- | --- |
| app | Interfaccia utente, onboarding e configurazione |
| agents | Ruoli, prompt, orchestrazione e strumenti |
| sources | Connettori e normalizzazione delle fonti |
| strategies | Specifiche, versioni e runtime dei segnali |
| data | Storico, feed live e qualità |
| research | Runner, esperimenti, backtest e report |
| portfolio | Allocazione, ledger e attribuzione delle posizioni |
| risk | Mandato e controlli deterministici |
| execution | Adattatori paper/live, ordini e riconciliazione |
| tests / examples | Verifiche significative e casi riproducibili |

## Decisioni aperte

| Decisione | Perché serve | Quando risolverla |
| --- | --- | --- |
| Primo mercato e strumenti | Determinano dati, costi, sessioni e semantica degli ordini | Fase 0 |
| Interfaccia iniziale: CLI, web o desktop | Determina onboarding e flusso di revisione | Fase 0 |
| Linguaggio e versione runtime | Compatibilità con motori, provider e librerie quant | Fase 0 |
| Framework agentico e modelli | Tool calling, output strutturati, checkpoint, costi | Fase 0 |
| Provider storico e motore di backtest | Copertura, qualità, licenze e riproducibilità | Fase 0 |
| Archivio, job e hosting | Persistenza degli esperimenti e isolamento | Fase 0–1 |
| Soglie di validazione e budget di ricerca | Evitare criteri scelti dopo aver visto i risultati | Prima degli esperimenti |
| Recupero di video e contenuti social | Accessibilità, credenziali, trascrizione e visione | Prima di ciascun connettore |
| Feed live e primo broker/exchange | Paper, ordini, limiti e riconciliazione | Prima della fase 4 |
| Metodo di allocazione e conflitti tra strategie | Tradurre segnali in esposizioni coerenti | Prima della fase 4 |
| Mandato e approvazione operativa | Definire autonomia, limiti e azioni di arresto | Prima della fase 5 |
| API verso Trading Sphere | Identità condivise e scambio di risultati/segnali | Quando serve la prima integrazione |

## Prossimo passo

Definire primo mercato, interfaccia e stack; poi formalizzare una strategia di esempio e il contratto dei dati. Queste scelte rendono implementabile la fase 1 senza anticipare l'esecuzione reale o costruire tutte le integrazioni contemporaneamente.
