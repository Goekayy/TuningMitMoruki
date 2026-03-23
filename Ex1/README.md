# Ex1 - Uploading Data to the Database (DB Tuning)

## Kurzüberblick
Dieses Projekt lädt `dblp/auth.tsv` in lokale Datenbanken und vergleicht mehrere Ladeverfahren. Verwendet wurden PostgreSQL und MariaDB.

- `naive`: ein `INSERT` pro Zeile
- `batch`: gruppierte `INSERT`s per `executemany`
- `copy`: PostgreSQL `COPY FROM STDIN`
- `load-data`: MariaDB `LOAD DATA LOCAL INFILE`

Dateien: `load_auth.py`, `load_auth_mariadb.py`

## Setup (lokal)
Stand der Messung: 2026-03-23

- OS: Windows (PowerShell)
- Python: 3.11.4
- psycopg: 3.3.3
- PyMySQL: 1.1.2
- PostgreSQL: 15.3
- MariaDB: 12.2.2
- DB-Host: `localhost` (`::1`), Port `5432`
- PostgreSQL-DB: `postgres`
- MariaDB-DB: `dbt_ex1` auf Port `3306`
- Datenquelle: `../dblp/auth.tsv` (3,095,201 Zeilen)

Die Datenbanken laufen lokal auf derselben Maschine. Das entspricht der Aufgabenempfehlung (keine Netzwerk-Latenz über externe Hosts).

## Installation
Aus dem Projekt-Root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r Ex1\requirements.txt
```

## Programmablauf (wie die Daten verarbeitet werden)
1. CLI-Parameter werden gelesen (`--method`, `--limit`, `--truncate`, ...).
2. Verbindung zu PostgreSQL wird aufgebaut.
3. Zieltabelle wird erstellt, falls sie nicht existiert:
   - `name VARCHAR(49)`
   - `pubid VARCHAR(129)`
4. Optional: Tabelle wird mit `TRUNCATE` geleert.
5. TSV wird zeilenweise gelesen:
   - Trennung am ersten Tab (`\t`) in `(name, pubid)`
   - leere/ungültige Zeilen werden übersprungen
6. Laden je nach Methode:
   - `naive`: pro Zeile `execute(INSERT...)`
   - `batch`: sammelt Zeilen in Blöcken, dann `executemany(...)`
   - `copy`: streamt Daten direkt via `COPY FROM STDIN`
7. Am Ende `COMMIT` und Ausgabe von Methode, Anzahl Zeilen, Laufzeit.

## Ausführung
Vom Projekt-Root starten (`.../Tuning`), damit der Default-Pfad `dblp/auth.tsv` passt.

```powershell
python Ex1\load_auth.py --method naive --limit 100000 --truncate --table auth_bench
python Ex1\load_auth.py --method batch --limit 100000 --truncate --table auth_bench --batch-size 5000
python Ex1\load_auth.py --method copy  --limit 100000 --truncate --table auth_bench
```

Vollimport mit `COPY`:

```powershell
python Ex1\load_auth.py --method copy --limit 0 --truncate --table auth_copy_full
```

MariaDB:

```powershell
python Ex1\load_auth_mariadb.py --method naive --limit 100000 --truncate --table auth_bench_maria
python Ex1\load_auth_mariadb.py --method batch --limit 100000 --truncate --table auth_bench_maria --batch-size 5000
python Ex1\load_auth_mariadb.py --method load-data --limit 100000 --truncate --table auth_bench_maria
python Ex1\load_auth_mariadb.py --method load-data --limit 0 --truncate --table auth_full_maria
```

## Gemessene Laufzeiten
Messung lokal am 2026-03-23:

- `naive`, 100000 Zeilen: `4.892 s`
- `batch`, 100000 Zeilen (`batch-size=5000`): `1.763 s`
- `copy`, 100000 Zeilen: `0.079 s`
- `copy`, Vollimport 3,095,201 Zeilen: `3.158 s`
- `mariadb naive`, 100000 Zeilen: `6.971 s`
- `mariadb batch`, 100000 Zeilen: `0.536 s`
- `mariadb load-data`, 100000 Zeilen: `0.312 s`
- `mariadb load-data`, Vollimport 3,095,201 Zeilen: `6.592 s`

Hinweis: Laut Aufgabenblatt ist bei der naiven Methode Teilmenge + lineare Hochrechnung akzeptiert (mit Kennzeichnung im Report).

## Quantitative Analyse
Aus den Messwerten lassen sich die Unterschiede noch klarer ableiten:

- `batch` ist gegenüber `naive` etwa `2.77x` schneller.
- `copy` ist gegenüber `naive` etwa `61.92x` schneller.
- `copy` ist gegenüber `batch` etwa `22.32x` schneller.
- `mariadb batch` ist gegenüber `mariadb naive` etwa `13.01x` schneller.
- `mariadb load-data` ist gegenüber `mariadb naive` etwa `22.34x` schneller.

Auch der Durchsatz in Zeilen pro Sekunde zeigt diesen Unterschied deutlich:

- `naive`: ca. `20,442 rows/s`
- `batch`: ca. `56,721 rows/s`
- `copy` bei 100000 Zeilen: ca. `1,265,823 rows/s`
- `copy` beim Vollimport: ca. `980,114 rows/s`

## Interpretation
Der straightforward Ansatz ist langsam, weil für jede einzelne Zeile ein separates `INSERT` ausgeführt wird. Auch wenn die gesamte Ladeoperation erst am Ende committed wird, fallen pro Tupel weiterhin erhebliche Fixkosten an. Dazu gehören Client-Server-Protokoll-Overhead, wiederholte Statement-Ausführung im Treiber, wiederholte Executor-Aufrufe im DBMS sowie MVCC- und WAL-Arbeit für jedes einzelne Tupel.

Der Batch-Ansatz reduziert diese Kosten, weil mehrere Tupel in größeren Blöcken verarbeitet werden. Dadurch sinkt die Zahl der separaten DB-Aufrufe und damit auch der Verwaltungsaufwand pro Tupel.

`COPY` ist nochmals deutlich schneller, weil PostgreSQL dafür einen spezialisierten Bulk-Load-Pfad besitzt. Die Daten werden nicht mehr als viele einzelne SQL-Statements behandelt, sondern als zusammenhängender Datenstrom. Dadurch entfallen große Teile des statementbezogenen Overheads. Zusätzlich verbessert sich die Cache-Lokalität, und die Datenbank kann die Tupelverarbeitung in einer kompakteren internen Pipeline abwickeln.

Wichtig ist auch, was hier nicht der Hauptfaktor war: Auf der Zieltabelle wurden keine zusätzlichen Sekundärindizes definiert. Der wesentliche Unterschied lag daher tatsächlich in der Art des Imports und nicht in teurer Index-Maintenance.

## Limitierungen
Die Messung ist sinnvoll, aber nicht vollständig frei von Einflussfaktoren:

- Der Zustand von OS- und Dateicache kann zwischen Läufen Unterschiede verursachen.
- Es wurde mit den Default-Einstellungen der lokalen PostgreSQL-Instanz gearbeitet.
- Es wurde auch bei MariaDB mit Default-Einstellungen gearbeitet.
- Die Tabelle war vor jedem Lauf leer und hatte keine zusätzlichen Indizes.

Diese Punkte ändern die Kernaussage nicht, begrenzen aber die Vergleichbarkeit absoluter Laufzeiten.

---

## Report-Text (Fragen 1-5, direkt verwendbar, DEUTSCH)

### 1) Beschreiben Sie die zwei effizienten Ansätze, die Sie implementiert haben.
Neben dem straightforward Ansatz wurden zwei effizientere Verfahren implementiert.

- Batch-Insert (`executemany`): Mehrere Tupel werden in Python zwischengespeichert und blockweise an die Datenbank übergeben. Dadurch sinkt der Overhead gegenüber einem einzelnen `INSERT` pro Zeile.
- PostgreSQL `COPY FROM STDIN`: Die Daten werden direkt an den Bulk-Loader von PostgreSQL übergeben. Diese Methode vermeidet die wiederholte Ausführung vieler einzelner SQL-Statements und nutzt den optimierten Importpfad des DBMS.

### 2) Geben Sie die Laufzeit für `auth.tsv` mit dem Straightforward-Ansatz und den effizienten Ansätzen an.
Messungen auf lokaler Maschine (PostgreSQL 15.3, Python 3.11.4, psycopg 3.3.3):

- Straightforward (`naive`): 100000 Zeilen in `4.892 s`
- Effizient 1 (`batch`): 100000 Zeilen in `1.763 s`
- Effizient 2 (`copy`): 100000 Zeilen in `0.079 s`
- Vollimport-Referenz: `copy` hat 3,095,201 Zeilen in `3.158 s` geladen

Die Straightforward-Methode wurde auf einer Teilmenge gemessen. Laut Aufgabenblatt ist die lineare Hochrechnung zulässig, wenn dies explizit angegeben wird.

Die quantitative Auswertung zeigt:

- `batch` ist ca. `2.77x` schneller als `naive`
- `copy` ist ca. `61.92x` schneller als `naive`
- `copy` erreicht bei 100000 Zeilen einen Durchsatz von ca. `1.27 Mio. rows/s`

### 3) Warum sind die effizienten Ansätze schneller?
Die effizienteren Ansätze sind schneller, weil sie den Overhead pro Tupel reduzieren und die Datenbank mehr Arbeit in größeren Einheiten ausführen kann.

Beim straightforward Ansatz wird für jede Zeile ein separates `INSERT` verarbeitet. Dadurch entstehen viele einzelne Statement-Grenzen, viele Protokoll-Interaktionen zwischen Client und Server und viele wiederholte Executor-Aufrufe. Zusätzlich muss PostgreSQL für jedes Tupel MVCC- und WAL-Arbeit leisten.

Beim Batch-Ansatz werden mehrere Tupel gemeinsam übergeben. Dadurch sinken die Zahl der separaten DB-Aufrufe und der damit verbundene Verwaltungsaufwand.

Bei `COPY` wird der SQL-Statement-Overhead fast vollständig umgangen, weil PostgreSQL einen spezialisierten Bulk-Import-Pfad nutzt. Dadurch gibt es deutlich weniger Client-Server-Protokoll-Overhead, weniger statementbezogene Ausführungskosten und eine bessere interne Datenverarbeitung mit günstigerer Cache-Lokalität.

### 4) Welches Tuning-Prinzip wurde angewendet?
Das wichtigste angewendete Tuning-Prinzip ist:

**Start-up costs are high; running costs are low.**

Der straightforward Ansatz erzeugt für jede Zeile erneut Start-up-Overhead. Die beiden effizienteren Verfahren bündeln Arbeit und reduzieren damit genau diese Kosten.

Zusätzlich wurde auch das Prinzip

**Render on the server what is due on the server**

angewendet, weil der eigentliche Bulk-Load bei `COPY` weitgehend vom DBMS selbst übernommen wird.

### 5) Wie wurde die Portabilität des Codes sichergestellt?
Die Portabilität wurde durch eine klare Trennung von allgemeiner und datenbankspezifischer Logik erreicht und praktisch mit MariaDB getestet.

- Allgemein und wiederverwendbar sind Argumentverarbeitung, Dateilesen, Zeilen-Parsing und der Kontrollfluss des Programms.
- Datenbankspezifisch sind der verwendete Treiber (`psycopg` bzw. `PyMySQL`), der Verbindungsaufbau und die Syntax für Bulk-Load.

Beim Wechsel auf MariaDB mussten vor allem Treiber und Bulk-Load-Anweisung angepasst werden. Während PostgreSQL `COPY FROM STDIN` verwendet, wurde in MariaDB `LOAD DATA LOCAL INFILE` genutzt.

Der grundlegende Ablauf des Programms blieb unverändert. Qualitativ zeigte MariaDB dieselbe Performance-Hierarchie wie PostgreSQL: straightforward am langsamsten, Batch schneller, nativer Bulk-Load am schnellsten.

## Portability-Check (aktueller Stand)
Ein zweites DBMS wurde mit MariaDB 12.2.2 lokal installiert und gemessen. Die Grundidee der Optimierung erwies sich dabei als portabel, auch wenn sich die absoluten Laufzeiten zwischen den Systemen unterscheiden.


