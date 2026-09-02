import importlib
import json
import runpy
import sys
import builtins
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from app.ai_scraper import ai_agent, config, http_client, main, scraper


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
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    reloaded = importlib.reload(config)

    package = importlib.import_module("app.ai_scraper")
    assert reloaded.OPENAI_API_KEY is None
    assert reloaded.OPENAI_MODEL == "gpt-4-turbo"
    assert reloaded.TRANSACTION_TYPES == {"sales": "venda", "rentals": "aluguel"}
    assert package.__version__ == "1.0.0"
    assert "AIScraper" in package.__all__


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
    instance = scraper.AIScraper.__new__(scraper.AIScraper)
    instance.raw_data_dir = str(tmp_path / "pages")

    assert instance.count_properties_in_html('data-id="1" data-id="2" data-id="1"') == 2
    assert instance.count_properties_in_html("<html></html>") == 0
    saved = instance.save_page_html(2, "<html>saved</html>")
    assert saved.name == "page_002.html"
    assert saved.read_text(encoding="utf-8") == "<html>saved</html>"


def test_scraper_constructor(monkeypatch):
    client = FakeHTTPClient([])
    monkeypatch.setattr(scraper, "HTTPClient", lambda: client)

    instance = scraper.AIScraper()

    assert instance.http_client is client
    assert instance.transaction_type is None
    assert instance.raw_data_dir is None


def test_scraper_saves_pages_and_stops_at_empty_page(monkeypatch, tmp_path):
    instance = scraper.AIScraper.__new__(scraper.AIScraper)
    instance.http_client = FakeHTTPClient(['data-id="1"', "<html></html>"])
    monkeypatch.setattr(scraper, "RAW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(scraper, "MAX_PAGES", None)

    pages = instance.scrape_transaction_type("rentals")

    assert len(pages) == 2
    assert Path(pages[0]).exists()
    assert instance.http_client.urls[0].endswith("/aluguel/df/todos/apartamento?pagina=1")


def test_scraper_stops_on_fetch_failure_and_max_pages(monkeypatch, tmp_path):
    failed = scraper.AIScraper.__new__(scraper.AIScraper)
    failed.http_client = FakeHTTPClient([None])
    monkeypatch.setattr(scraper, "RAW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(scraper, "MAX_PAGES", None)
    assert failed.scrape_transaction_type("sales") == []

    limited = scraper.AIScraper.__new__(scraper.AIScraper)
    limited.http_client = FakeHTTPClient(['data-id="1"'])
    monkeypatch.setattr(scraper, "MAX_PAGES", 1)
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
    monkeypatch.setattr(main, "AIScraper", scraper_class)
    monkeypatch.setattr(sys, "argv", ["main.py", "--type", "rentals"])
    assert main.main() == expected


def test_main_module_exits_with_cli_status(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--help"])
    with pytest.raises(SystemExit) as result:
        runpy.run_module("app.ai_scraper.main", run_name="__main__")
    assert result.value.code == 0