.PHONY: install test oracle clean

# Install dependencies
install:
	composer install

# Run unit tests
test:
	./vendor/bin/phpunit

# Run unit tests with verbose output
test-verbose:
	./vendor/bin/phpunit --verbose

# Run oracle tests
oracle:
	cd ../oracle/runners && php php_runner.php

# Run oracle tests with verbose output
oracle-verbose:
	cd ../oracle/runners && php php_runner.php -v

# Clean vendor directory
clean:
	rm -rf vendor composer.lock

# All tests
all: test oracle
