.PHONY: build test clean release

# Build the SDK
build:
	dotnet build

# Run tests
test:
	dotnet test

# Clean build artifacts
clean:
	dotnet clean
	rm -rf bin obj tests/bin tests/obj

# Build release package
release:
	dotnet build -c Release

# Pack NuGet package
pack:
	dotnet pack -c Release

# Restore dependencies
restore:
	dotnet restore

# Format code
format:
	dotnet format

# Run oracle tests (requires YamlDotNet)
test-oracle:
	cd ../oracle/runners && dotnet run --project DotNetOracleRunner.csproj -- ../spec/

help:
	@echo "Available targets:"
	@echo "  build       - Build the SDK"
	@echo "  test        - Run unit tests"
	@echo "  clean       - Clean build artifacts"
	@echo "  release     - Build release configuration"
	@echo "  pack        - Create NuGet package"
	@echo "  restore     - Restore dependencies"
	@echo "  format      - Format code"
	@echo "  test-oracle - Run oracle tests"
