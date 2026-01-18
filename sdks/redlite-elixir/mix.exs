defmodule Redlite.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/russellromney/redlite"

  def project do
    [
      app: :redlite,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.37"},
      {:rustler_precompiled, "~> 0.8"},
      {:yaml_elixir, "~> 2.9", only: [:dev, :test]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    Redis-compatible embedded database with SQLite durability.
    Provides Redis API commands with automatic persistence via SQLite.
    """
  end

  defp package do
    [
      name: "redlite",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib native .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: ["README.md", "CHANGELOG.md"]
    ]
  end
end
