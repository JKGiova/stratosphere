# Stratosphere

**Strategie quantitative create, testate e gestite attraverso agenti.**

Stratosphere è il software della suite **Trading Sphere** dedicato al ciclo completo di una strategia quant: partire da un'idea o da una fonte esterna, trasformarla in regole eseguibili, verificarla su dati storici e in tempo reale, migliorarla e attivarla su un conto reale con gestione coordinata del portfolio.

Le fonti previste comprendono video YouTube, Reel Instagram, articoli, post e thread X, strategie TradingView e repository GitHub. Gli agenti ricostruiscono le regole, evidenziano le informazioni mancanti e propongono implementazioni e varianti. Un agente di portfolio management valuta insieme i segnali delle strategie attive e propone allocazioni, aperture, riduzioni e chiusure delle posizioni, entro limiti applicati dal sistema di rischio.

## Stato del progetto

**Fase di pianificazione.** Il repository contiene il piano iniziale e la struttura della documentazione. Non contiene ancora un'applicazione funzionante, connettori, motori di backtest o integrazioni per inviare ordini. Le funzionalità descritte sono obiettivi di sviluppo; stack, mercati e primo broker restano da scegliere.

## Funzionalità previste

- Importazione di fonti e conservazione dei riferimenti alle regole estratte.
- Creazione, modifica e miglioramento di strategie tramite agenti specializzati.
- Specifiche e implementazioni versionate, con ipotesi e parametri espliciti.
- Backtest riproducibili con costi, controlli sui dati e validazione fuori campione.
- Forward test e paper trading su dati in tempo reale.
- Gestione congiunta di capitale, esposizioni e posizioni di più strategie.
- Attivazione reale di versioni validate, con monitoraggio, riconciliazione e arresto operativo.

## Percorso di una strategia

1. Importare una fonte o descrivere un'idea.
2. Estrarre le regole e risolvere le ambiguità.
3. Generare una specifica e un'implementazione verificabili.
4. Eseguire backtest, validazione e confronti tra versioni.
5. Provare la versione selezionata con dati live e capitale simulato.
6. Attivarla esplicitamente per il trading reale entro un mandato di rischio.
7. Monitorare risultati e posizioni; sviluppare eventuali miglioramenti come nuove versioni.

Il risultato di un backtest e le affermazioni della fonte non costituiscono una garanzia di rendimento futuro.

## Documentazione

| Documento | Contenuto |
| --- | --- |
| [Indice del piano](docs/README.md) | Percorso di lettura e stato dei documenti |
| [Visione e prodotto](docs/product-plan.md) | Obiettivi, casi d'uso, confini e requisiti |
| [Architettura e agenti](docs/architecture.md) | Componenti, responsabilità e contratti |
| [Fonti e strategie](docs/strategy-lifecycle.md) | Importazione, specifica, versioni e miglioramenti |
| [Dati e validazione](docs/testing-and-validation.md) | Backtest, prove live e criteri di promozione |
| [Portfolio ed esecuzione](docs/portfolio-and-execution.md) | Allocazione, posizioni, rischio e ordini |
| [Roadmap e decisioni](docs/roadmap.md) | MVP, fasi successive e scelte aperte |

## Primo obiettivo di sviluppo

Completare un percorso riproducibile da **articolo o testo fornito dall'utente → specifica → strategia eseguibile → backtest → report**, su un solo mercato e un solo formato dati. Le integrazioni multimediali, il paper trading e l'esecuzione reale seguiranno nelle fasi definite dalla [roadmap](docs/roadmap.md).
