import importlib
import json
import runpy
import sys
import builtins
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import pytest

from app.ai_scraper import ai_agent, config, http_client
from app.ai_scraper.ad_links_collector import ad_links_collector, main as main_pages_main
from app.ai_scraper.property_pages_downloader import main as property_pages_main, property_pages_downloader


class FakeOpenAI:
    def __init__(self, content=None, error=None):
        self.content = content
        self.error = error
        self.calls = []
        self.chat = SimpleNamespace(completions=self)

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=self.content))]
        )


class FakeMainPagesHTTPClient:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.urls = []
        self.closed = False

    def get(self, url):
        self.urls.append(url)
        return next(self.pages)

    def close(self):
        self.closed = True


def make_agent(monkeypatch, content=None, error=None):
    fake_client = FakeOpenAI(content=content, error=error)
    monkeypatch.setattr(ai_agent, "OpenAI", lambda api_key: fake_client)
    return ai_agent.AIScrapingAgent(model="test-model"), fake_client


def test_config_defaults_and_exports(monkeypatch):
    import dotenv
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    reloaded = importlib.reload(config)

    package = importlib.import_module("app.ai_scraper")
    assert reloaded.OPENAI_API_KEY is None
    assert reloaded.OPENAI_MODEL == "gpt-4-turbo"
    assert reloaded.TRANSACTION_TYPES == {"sales": "venda", "rentals": "aluguel"}
    assert package.__version__ == "1.0.0"
    assert "AdLinksCollector" in package.__all__


def test_config_falls_back_to_dotenv_search_when_no_candidate_env_file_exists(monkeypatch):
    import dotenv
    monkeypatch.setattr(config.Path, "exists", lambda path: False)
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: False)
    importlib.reload(config)


def test_config_loads_existing_environment_file_and_handles_missing_dotenv(monkeypatch):
    monkeypatch.setattr(config.Path, "exists", lambda path: True)
    importlib.reload(config)

    original_import = builtins.__import__

    def reject_dotenv(name, *args, **kwargs):
        if name == "dotenv":
            raise ImportError("dotenv unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_dotenv)
    importlib.reload(config)


def test_http_client_returns_content_waits_and_closes(monkeypatch):
    class FakeHeaders:
        @staticmethod
        def get_content_charset():
            return "utf-8"

    class FakeResponse:
        headers = FakeHeaders()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        @staticmethod
        def read():
            return b"page"

    calls = []
    monkeypatch.setattr(http_client, "urlopen", lambda request, timeout: calls.append((request, timeout)) or FakeResponse())
    clock = iter([10, 10, 13])
    monkeypatch.setattr(http_client.time, "time", lambda: next(clock))
    sleeps = []
    monkeypatch.setattr(http_client.time, "sleep", sleeps.append)

    client = http_client.HTTPClient(delay=5)
    client.last_request_time = 8
    assert client.get("https://example.test") == "page"
    assert sleeps == [3]
    assert calls[0][0].full_url == "https://example.test"
    assert calls[0][1] == config.REQUEST_TIMEOUT
    client.close()


def test_http_client_returns_none_for_request_errors(monkeypatch, capsys):
    monkeypatch.setattr(http_client, "urlopen", lambda request, timeout: (_ for _ in ()).throw(URLError("offline")))
    monkeypatch.setattr(http_client.time, "time", lambda: 10)
    client = http_client.HTTPClient(delay=0)

    assert client.get("https://example.test") is None
    assert "Error fetching https://example.test" in capsys.readouterr().out


def test_http_client_returns_none_for_bad_status(monkeypatch, capsys):
    monkeypatch.setattr(http_client, "urlopen", lambda request, timeout: (_ for _ in ()).throw(HTTPError("url", 404, "not found", {}, None)))
    monkeypatch.setattr(http_client.time, "time", lambda: 10)
    client = http_client.HTTPClient(delay=0)

    assert client.get("https://example.test") is None
    assert "Error fetching https://example.test" in capsys.readouterr().out


def test_agent_parses_json_and_optional_response_format(monkeypatch):
    agent, client = make_agent(monkeypatch, '{"links": ["one"]}')

    assert agent._call_openai("prompt", {"type": "json_object"}) == {"links": ["one"]}
    assert client.calls[0]["model"] == "test-model"
    assert client.calls[0]["response_format"] == {"type": "json_object"}


def test_agent_parses_fenced_and_embedded_json(monkeypatch):
    agent, _ = make_agent(monkeypatch, 'prefix ```json\n{"one": 1}\n``` suffix')
    assert agent._call_openai("prompt") == {"one": 1}

    agent.client.content = 'answer: {"two": 2} done'
    assert agent._call_openai("prompt") == {"two": 2}


def test_agent_returns_raw_response_and_errors(monkeypatch, capsys):
    agent, _ = make_agent(monkeypatch, "not json")
    assert agent._call_openai("prompt") == {"raw_response": "not json"}

    agent.client.error = RuntimeError("service unavailable")
    assert agent._call_openai("prompt") == {"error": "service unavailable"}
    assert "Error calling OpenAI" in capsys.readouterr().out


def test_agent_handles_incomplete_and_invalid_json_wrappers(monkeypatch):
    agent, _ = make_agent(monkeypatch, "```json")
    assert agent._call_openai("prompt") == {"raw_response": "```json"}

    agent.client.content = "}{"
    assert agent._call_openai("prompt") == {"raw_response": "}{"}

    agent.client.content = "{invalid json}"
    assert "error" in agent._call_openai("prompt")


def test_agent_builds_extraction_prompts_and_validates(monkeypatch):
    agent, _ = make_agent(monkeypatch, '{"title": "Home", "link": "https://example.test/1"}')
    assert agent.extract_property_details("detail", "https://example.test/1")["title"] == "Home"
    assert agent.validate_extraction({"title": "Home", "link": "url"})
    assert not agent.validate_extraction({"error": "bad"})
    assert not agent.validate_extraction({"title": "Home"})


def test_save_links_writes_json_file(tmp_path, monkeypatch):
    monkeypatch.setattr(ad_links_collector, "RAW_DATA_DIR", str(tmp_path))
    instance = ad_links_collector.AdLinksCollector.__new__(ad_links_collector.AdLinksCollector)

    links = ["https://example.test/imovel/1", "https://example.test/imovel/2"]
    output_path = instance.save_links("rentals", [{"page": 1, "links": links}])

    assert output_path == tmp_path / "rentals" / "links.json"
    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved == [{"page": 1, "links": links}]


def test_extract_property_links_deduplicates_and_builds_full_urls_for_main_pages():
    html = (
        'https://www.dfimoveis.com.br/imovel/apartamento-1-quarto-aluguel-asa-sul-1375580 '
        'https://www.dfimoveis.com.br/imovel/apartamento-1-quarto-aluguel-asa-sul-1375580 '
        'href="/imovel/apartamento-4-quartos-aluguel-park-sul-1328253" '
        '<a href=/imovel/apartamento-1-quarto-aluguel-sul-aguas-claras-df-rua-17-1424764> '
        '<a href="/imovel/apartamento-2-quartos-aluguel-areal-aguas-claras-df-qs-5-rua-310-1422425'
    )
    links = ad_links_collector.AdLinksCollector.collect_page_ad_links(html)
    assert len(links) == 4
    assert ad_links_collector.AdLinksCollector.collect_page_ad_links("<html></html>") == []


def test_scraper_constructor(monkeypatch):
    http = FakeMainPagesHTTPClient([])
    monkeypatch.setattr(ad_links_collector, "HTTPClient", lambda: http)

    instance = ad_links_collector.AdLinksCollector()

    assert instance.http_client is http
    assert instance.transaction_type is None


def test_scraper_saves_pages_and_stops_at_empty_page(tmp_path, monkeypatch):
    monkeypatch.setattr(ad_links_collector, "RAW_DATA_DIR", str(tmp_path))
    instance = ad_links_collector.AdLinksCollector.__new__(ad_links_collector.AdLinksCollector)
    instance.http_client = FakeMainPagesHTTPClient([
        'href="/imovel/apartamento-1-quarto-aluguel-asa-sul-1375580"',
        "<html></html>",
    ])

    pages = instance.collect("rentals")

    assert len(pages) == 1
    assert pages[0]["page"] == 1
    assert len(pages[0]["links"]) == 1
    assert instance.http_client.urls[0].endswith("/aluguel/df/todos/apartamento?pagina=1")
    assert len(instance.http_client.urls) == 2

    saved = json.loads((tmp_path / "rentals" / "links.json").read_text(encoding="utf-8"))
    assert saved == pages


def test_scraper_stops_on_fetch_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(ad_links_collector, "RAW_DATA_DIR", str(tmp_path))
    failed = ad_links_collector.AdLinksCollector.__new__(ad_links_collector.AdLinksCollector)
    failed.http_client = FakeMainPagesHTTPClient([None])
    assert failed.collect("sales") == []
    failed.close()
    assert failed.http_client.closed


@pytest.mark.parametrize(
    ("scraper_class", "expected"),
    [
        (lambda: SimpleNamespace(collect=lambda kind: [kind], close=lambda: None), 0),
        (lambda: (_ for _ in ()).throw(KeyboardInterrupt()), 130),
        (lambda: (_ for _ in ()).throw(RuntimeError("broken")), 1),
    ],
)
def test_main_returns_expected_exit_codes(monkeypatch, scraper_class, expected):
    monkeypatch.setattr(main_pages_main, "AdLinksCollector", scraper_class)
    monkeypatch.setattr(sys, "argv", ["main.py", "--type", "rentals"])
    assert main_pages_main.main() == expected


def test_main_module_exits_with_cli_status(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--help"])
    with pytest.raises(SystemExit) as result:
        runpy.run_module("app.ai_scraper.ad_links_collector.main", run_name="__main__")
    assert result.value.code == 0


@pytest.mark.parametrize(
    "script_path",
    [
        Path("app/ai_scraper/ad_links_collector/main.py"),
        Path("app/ai_scraper/property_pages_downloader/main.py"),
    ],
)
def test_downloader_scripts_support_direct_execution(monkeypatch, script_path):
    monkeypatch.setattr(sys, "argv", [str(script_path), "--help"])
    with pytest.raises(SystemExit) as result:
        runpy.run_path(str(script_path), run_name="__main__")
    assert result.value.code == 0


def test_agent_extracts_property_page_details(monkeypatch):
    agent, client = make_agent(monkeypatch, '{"link": "https://example.test/1", "title": "Apto"}')

    result = agent.extract_property_page_details("<html>detail</html>", "https://example.test/1")

    assert result == {"link": "https://example.test/1", "title": "Apto"}
    assert "<html>detail</html>" in client.calls[0]["messages"][0]["content"]


class FakeDetailHTTPClient:
    def __init__(self, pages):
        self.pages = dict(pages)
        self.urls = []
        self.closed = False

    def get(self, url):
        self.urls.append(url)
        return self.pages.get(url)

    def close(self):
        self.closed = True


def test_extract_property_links_deduplicates_and_builds_full_urls():
    html = (
        '<a href="/imovel/apto-1">1</a>'
        '<a href="/imovel/apto-1">duplicate</a>'
        '<a href=/imovel/apto-2>unquoted 2</a>'
        '<a href=\'/imovel/apto-3\'>single quoted 3</a>'
        '<a href="https://example.test/imovel/apto-4">absolute 4</a>'
        '<a href="/mapa?negocio=aluguel">not an ad</a>'
    )

    links = property_pages_downloader.PropertyPagesDownloader.extract_property_links(html, base_url="https://example.test")

    assert links == [
        "https://example.test/imovel/apto-1",
        "https://example.test/imovel/apto-2",
        "https://example.test/imovel/apto-3",
        "https://example.test/imovel/apto-4",
    ]


def test_extract_property_downloads_and_saves_html(tmp_path):
    http_client = FakeDetailHTTPClient({"https://example.test/imovel/apto-1": "<html>detail</html>"})
    extractor = property_pages_downloader.PropertyPagesDownloader(http_client=http_client)

    output_path = extractor.extract_property(
        "https://example.test/imovel/apto-1", output_dir=tmp_path
    )

    assert output_path.parent == tmp_path
    assert output_path.read_text(encoding="utf-8") == "<html>detail</html>"
    assert output_path.suffix == ".html"
    assert http_client.urls == ["https://example.test/imovel/apto-1"]


def test_extract_property_returns_none_on_fetch_failure(tmp_path):
    http_client = FakeDetailHTTPClient({})
    extractor = property_pages_downloader.PropertyPagesDownloader(http_client=http_client)
    assert extractor.extract_property("https://example.test/imovel/missing") is None

    assert list(tmp_path.iterdir()) == []


def test_extract_properties_from_page_visits_each_link(tmp_path):
    html = '<a href="/imovel/apto-1">1</a><a href="/imovel/apto-2">2</a>'
    http_client = FakeDetailHTTPClient({
        "https://example.test/imovel/apto-1": "<html>one</html>",
        "https://example.test/imovel/apto-2": None,
    })
    extractor = property_pages_downloader.PropertyPagesDownloader(http_client=http_client)

    properties = extractor.extract_properties_from_page(
        html, base_url="https://example.test", output_dir=tmp_path
    )

    assert len(properties) == 1
    assert properties[0].read_text(encoding="utf-8") == "<html>one</html>"


def test_extractor_uses_default_http_client(monkeypatch):
    fake_http_client = FakeDetailHTTPClient({})
    monkeypatch.setattr(property_pages_downloader, "HTTPClient", lambda: fake_http_client)

    extractor = property_pages_downloader.PropertyPagesDownloader()

    assert extractor.http_client is fake_http_client


def test_extractor_close_closes_http_client():
    http_client = FakeDetailHTTPClient({})
    extractor = property_pages_downloader.PropertyPagesDownloader(http_client=http_client)

    extractor.close()

    assert http_client.closed


def test_load_page_paths_sorts_pages(tmp_path, monkeypatch):
    monkeypatch.setattr(property_pages_downloader, "RAW_DATA_DIR", str(tmp_path))
    pages_dir = tmp_path / "rentals" / "pages"
    pages_dir.mkdir(parents=True)
    (pages_dir / "page_002.html").write_text("two", encoding="utf-8")
    (pages_dir / "page_001.html").write_text("one", encoding="utf-8")

    all_paths = property_pages_downloader.PropertyPagesDownloader._load_page_paths("rentals")
    assert [p.name for p in all_paths] == ["page_001.html", "page_002.html"]


def test_extract_transaction_type_downloads_property_pages(tmp_path, monkeypatch):
    monkeypatch.setattr(property_pages_downloader, "RAW_DATA_DIR", str(tmp_path / "raw"))
    pages_dir = tmp_path / "raw" / "rentals" / "pages"
    pages_dir.mkdir(parents=True)
    pages_dir.joinpath("page_001.html").write_text(
        '<a href="/imovel/apto-1">1</a>', encoding="utf-8"
    )

    http_client = FakeDetailHTTPClient({"https://www.dfimoveis.com.br/imovel/apto-1": "<html>one</html>"})
    extractor = property_pages_downloader.PropertyPagesDownloader(http_client=http_client)

    properties = extractor.extract_transaction_type("rentals")

    assert len(properties) == 1
    assert properties[0].read_text(encoding="utf-8") == "<html>one</html>"
    assert properties[0].parent == tmp_path / "raw" / "rentals" / "properties"


@pytest.mark.parametrize(
    ("extractor_class", "expected"),
    [
        (
            lambda: SimpleNamespace(
                extract_transaction_type=lambda kind: [kind], close=lambda: None
            ),
            0,
        ),
        (lambda: (_ for _ in ()).throw(KeyboardInterrupt()), 130),
        (lambda: (_ for _ in ()).throw(RuntimeError("broken")), 1),
    ],
)
def test_extract_main_returns_expected_exit_codes(monkeypatch, extractor_class, expected):
    monkeypatch.setattr(property_pages_main, "PropertyPagesDownloader", extractor_class)
    monkeypatch.setattr(sys, "argv", ["main.py", "--type", "rentals"])
    assert property_pages_main.main() == expected


def test_extract_main_module_exits_with_cli_status(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--help"])
    with pytest.raises(SystemExit) as result:
        runpy.run_module(
            "app.ai_scraper.property_pages_downloader.main", run_name="__main__"
        )
    assert result.value.code == 0