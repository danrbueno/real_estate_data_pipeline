import importlib
import json
import os
import runpy
import sys
import builtins
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from app.ai_scraper import ai_agent, config, http_client
from app.ai_scraper.main_pages_downloader import main as main_pages_main, main_pages_downloader
from app.ai_scraper.property_pages_downloader import main as property_pages_main, property_pages_downloader


class FakeResponse:
    def __init__(self, text="<html></html>", error=None):
        self.text = text
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.closed = False
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def close(self):
        self.closed = True


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


class FakeHTTPClient:
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
    assert "AIScraper" in package.__all__


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
    fake_session = FakeSession(FakeResponse("page"))
    monkeypatch.setattr(http_client.requests, "Session", lambda: fake_session)
    clock = iter([10, 10, 13])
    monkeypatch.setattr(http_client.time, "time", lambda: next(clock))
    sleeps = []
    monkeypatch.setattr(http_client.time, "sleep", sleeps.append)

    client = http_client.HTTPClient(delay=5)
    client.last_request_time = 8
    assert client.get("https://example.test") == "page"
    client.close()

    assert sleeps == [3]
    assert fake_session.calls == [("https://example.test", config.REQUEST_TIMEOUT)]
    assert fake_session.closed


def test_http_client_returns_none_for_request_errors(monkeypatch, capsys):
    fake_session = FakeSession(requests.RequestException("offline"))
    monkeypatch.setattr(http_client.requests, "Session", lambda: fake_session)
    monkeypatch.setattr(http_client.time, "time", lambda: 10)
    client = http_client.HTTPClient(delay=0)

    assert client.get("https://example.test") is None
    assert "Error fetching https://example.test" in capsys.readouterr().out


def test_http_client_returns_none_for_bad_status(monkeypatch, capsys):
    fake_response = FakeResponse(error=requests.HTTPError("404 Client Error"))
    fake_session = FakeSession(fake_response)
    monkeypatch.setattr(http_client.requests, "Session", lambda: fake_session)
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
    agent, _ = make_agent(monkeypatch, '{"links": ["https://example.test/1"]}')
    assert agent.extract_property_links("listing", "https://example.test") == ["https://example.test/1"]

    agent.client.content = '{"has_next_page": false, "total_in_page": 0, "page_is_empty": true}'
    assert agent.extract_pagination_info("listing")["page_is_empty"] is True

    agent.client.content = '{"title": "Home", "link": "https://example.test/1"}'
    assert agent.extract_property_details("detail", "https://example.test/1")["title"] == "Home"
    assert agent.validate_extraction({"title": "Home", "link": "url"})
    assert not agent.validate_extraction({"error": "bad"})
    assert not agent.validate_extraction({"title": "Home"})


def test_count_properties_and_save_page(tmp_path):
    instance = main_pages_downloader.AIScraper.__new__(main_pages_downloader.AIScraper)
    instance.raw_data_dir = str(tmp_path / "pages")

    assert instance.count_properties_in_html('data-id="1" data-id="2" data-id="1"') == 2
    assert instance.count_properties_in_html("<html></html>") == 0
    saved = instance.save_page_html(2, "<html>saved</html>")
    assert saved.name == "page_002.html"
    assert saved.read_text(encoding="utf-8") == "<html>saved</html>"


def test_scraper_constructor(monkeypatch):
    client = FakeHTTPClient([])
    monkeypatch.setattr(main_pages_downloader, "HTTPClient", lambda: client)

    instance = main_pages_downloader.AIScraper()

    assert instance.http_client is client
    assert instance.transaction_type is None
    assert instance.raw_data_dir is None


def test_scraper_saves_pages_and_stops_at_empty_page(monkeypatch, tmp_path):
    instance = main_pages_downloader.AIScraper.__new__(main_pages_downloader.AIScraper)
    instance.http_client = FakeHTTPClient(['data-id="1"', "<html></html>"])
    monkeypatch.setattr(main_pages_downloader, "RAW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(main_pages_downloader, "MAX_PAGES", None)

    pages = instance.scrape_transaction_type("rentals")

    assert len(pages) == 2
    assert Path(pages[0]).exists()
    assert instance.http_client.urls[0].endswith("/aluguel/df/todos/apartamento?pagina=1")


def test_scraper_stops_on_fetch_failure_and_max_pages(monkeypatch, tmp_path):
    failed = main_pages_downloader.AIScraper.__new__(main_pages_downloader.AIScraper)
    failed.http_client = FakeHTTPClient([None])
    monkeypatch.setattr(main_pages_downloader, "RAW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(main_pages_downloader, "MAX_PAGES", None)
    assert failed.scrape_transaction_type("sales") == []

    limited = main_pages_downloader.AIScraper.__new__(main_pages_downloader.AIScraper)
    limited.http_client = FakeHTTPClient(['data-id="1"'])
    monkeypatch.setattr(main_pages_downloader, "MAX_PAGES", 1)
    assert len(limited.scrape_transaction_type("sales")) == 1
    limited.close()
    assert limited.http_client.closed


@pytest.mark.parametrize(
    ("scraper_class", "expected"),
    [
        (lambda: SimpleNamespace(scrape_transaction_type=lambda kind: [kind], close=lambda: None), 0),
        (lambda: (_ for _ in ()).throw(KeyboardInterrupt()), 130),
        (lambda: (_ for _ in ()).throw(RuntimeError("broken")), 1),
    ],
)
def test_main_returns_expected_exit_codes(monkeypatch, scraper_class, expected):
    monkeypatch.setattr(main_pages_main, "AIScraper", scraper_class)
    monkeypatch.setattr(sys, "argv", ["main.py", "--type", "rentals"])
    assert main_pages_main.main() == expected


def test_main_module_exits_with_cli_status(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--help"])
    with pytest.raises(SystemExit) as result:
        runpy.run_module("app.ai_scraper.main_pages_downloader.main", run_name="__main__")
    assert result.value.code == 0


@pytest.mark.parametrize(
    "script_path",
    [
        Path("app/ai_scraper/main_pages_downloader/main.py"),
        Path("app/ai_scraper/property_pages_downloader/main.py"),
    ],
)
def test_downloader_scripts_support_direct_execution(monkeypatch, script_path):
    monkeypatch.setattr(sys, "argv", [str(script_path), "--help"])
    with pytest.raises(SystemExit) as result:
        runpy.run_path(str(script_path), run_name="__main__")
    assert result.value.code == 0


def test_config_exposes_processed_data_dir():
    assert os.path.basename(os.path.normpath(config.PROCESSED_DATA_DIR)) == "properties"


def test_agent_extracts_property_page_details(monkeypatch):
    agent, client = make_agent(monkeypatch, '{"link": "https://example.test/1", "title": "Apto"}')

    result = agent.extract_property_page_details("<html>detail</html>", "https://example.test/1")

    assert result == {"link": "https://example.test/1", "title": "Apto"}
    assert "<html>detail</html>" in client.calls[0]["messages"][0]["content"]


class FakeExtractionAgent:
    def __init__(self, results):
        self.results = iter(results)
        self.calls = []

    def extract_property_page_details(self, html, property_url):
        self.calls.append((html, property_url))
        return next(self.results)

    @staticmethod
    def validate_extraction(data):
        return "error" not in data and "link" in data


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


def test_property_downloader_test_fakes_record_calls():
    agent = FakeExtractionAgent([{"link": "url"}])
    assert agent.extract_property_page_details("html", "url") == {"link": "url"}
    assert agent.calls == [("html", "url")]
    assert agent.validate_extraction({"link": "url"})
    assert not agent.validate_extraction({"error": "bad"})

    client = FakeDetailHTTPClient({"url": "html"})
    assert client.get("url") == "html"
    client.close()
    assert client.closed


def test_extract_property_links_deduplicates_and_builds_full_urls():
    html = (
        '<a href="/imovel/apto-1">1</a>'
        '<a href="/imovel/apto-1">duplicate</a>'
        '<a href="/imovel/apto-2">2</a>'
        '<a href="/mapa?negocio=aluguel">not an ad</a>'
    )

    links = property_pages_downloader.PropertyPagesDownloader.extract_property_links(html, base_url="https://example.test")

    assert links == [
        "https://example.test/imovel/apto-1",
        "https://example.test/imovel/apto-2",
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


def test_load_page_paths_sorts_and_limits(tmp_path, monkeypatch):
    monkeypatch.setattr(property_pages_downloader, "RAW_DATA_DIR", str(tmp_path))
    pages_dir = tmp_path / "rentals" / "pages"
    pages_dir.mkdir(parents=True)
    (pages_dir / "page_002.html").write_text("two", encoding="utf-8")
    (pages_dir / "page_001.html").write_text("one", encoding="utf-8")

    all_paths = property_pages_downloader.PropertyPagesDownloader._load_page_paths("rentals")
    assert [p.name for p in all_paths] == ["page_001.html", "page_002.html"]

    limited = property_pages_downloader.PropertyPagesDownloader._load_page_paths("rentals", max_pages=1)
    assert [p.name for p in limited] == ["page_001.html"]


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
                extract_transaction_type=lambda kind, max_pages: [kind], close=lambda: None
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