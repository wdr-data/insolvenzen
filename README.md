# Insolvenzen

### clone repo

`git clone https://github.com/wdr-data/insolvenzen.git`

### install pipenv (for linux)

`sudo apt install pipenv`

### configure all dependencies in insolvenzen

`pipenv sync`

### if pipenv shell doesn't start, then execute

`pipenv shell`

### run scrapers for private Insolvenzen

`python -m insolvenzen.scarper.private`