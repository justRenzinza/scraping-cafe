import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from http.server import BaseHTTPRequestHandler


# =========================================================
# SITE 1 - Cooabriel
# =========================================================
def scrape_cooabriel():
	URL = "https://cooabriel.coop.br/cotacao-do-dia"
	PRODUTOS_ALVO = ["Conilon 7/8", "Cacau Tipo 1"]

	headers = {"User-Agent": "Mozilla/5.0"}
	response = requests.get(URL, headers=headers, timeout=10)
	response.raise_for_status()
	soup = BeautifulSoup(response.text, "html.parser")

	dados = []
	for row in soup.select("tr"):
		colunas = row.find_all("td")
		if len(colunas) < 4:
			continue

		tipo  = colunas[0].text.strip()
		data  = colunas[1].text.strip()
		hora  = colunas[2].text.strip()
		preco = colunas[3].text.strip()

		if tipo not in PRODUTOS_ALVO:
			continue

		texto_preco = (
			preco.replace("R$", "").replace(".", "").replace(",", ".").strip()
		)
		try:
			dados.append({
				"fonte": "Cooabriel",
				"produto": tipo,
				"data": data,
				"hora": hora,
				"preco": float(texto_preco)
			})
		except ValueError:
			continue

	return dados


# =========================================================
# SITE 2 - CCCV
# =========================================================
def scrape_cccv():
	URL = "https://cccv.org.br/cotacao/"

	session = requests.Session()
	session.headers.update({
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
		"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language": "pt-BR,pt;q=0.9",
		"Accept-Encoding": "gzip, deflate, br",
		"Referer": "https://cccv.org.br/",
		"Connection": "keep-alive",
		"Upgrade-Insecure-Requests": "1",
		"Cache-Control": "max-age=0",
	})

	session.get("https://cccv.org.br/", timeout=10)
	response = session.get(URL, timeout=10)
	response.raise_for_status()
	soup = BeautifulSoup(response.text, "html.parser")

	tabela = soup.find("table")
	if not tabela:
		return []

	linhas = tabela.find_all("tr")

	registros = {}
	for row in linhas:
		colunas = row.find_all("td")
		if len(colunas) < 4:
			continue
		try:
			dia = int(colunas[0].text.strip())
		except ValueError:
			continue

		registros[dia] = [col.text.strip() for col in colunas[1:4]]

	dia_atual = datetime.now().day
	dia_encontrado = None
	valores_encontrados = None

	for dia in range(dia_atual, 0, -1):
		if dia in registros:
			vals = registros[dia]
			if vals[0] != "-" and vals[1] != "-" and vals[2] != "-":
				dia_encontrado = dia
				valores_encontrados = vals
				break

	if not dia_encontrado:
		return []

	nomes = ["Arabica Duro", "Arabica Rio", "Conilon 7/8"]
	dados = []
	mes_ano = datetime.now().strftime("%m/%Y")

	for i, nome in enumerate(nomes):
		texto_preco = (
			valores_encontrados[i]
			.replace(".", "")
			.replace(",", ".")
			.strip()
		)
		try:
			dados.append({
				"fonte": "CCCV",
				"produto": nome,
				"data": f"{dia_encontrado:02d}/{mes_ano}",
				"hora": "-",
				"preco": float(texto_preco)
			})
		except ValueError:
			continue

	return dados


# =========================================================
# SITE 3 - Noticias Agricolas - Arabica Duro
# =========================================================
def scrape_noticias_agricolas_arabica_duro():
	URL = "https://www.noticiasagricolas.com.br/cotacoes/cafe/cafe-arabica-mercado-fisico-tipo-6-duro"

	session = requests.Session()
	session.headers.update({
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
		"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language": "pt-BR,pt;q=0.9",
		"Referer": "https://www.noticiasagricolas.com.br/",
	})

	response = session.get(URL, timeout=10)
	response.raise_for_status()
	soup = BeautifulSoup(response.text, "html.parser")

	tabela = soup.find("table")
	if not tabela:
		return []

	data_fechamento = ""
	for tag in soup.find_all(["h2", "h3", "p", "strong", "b"]):
		if "Fechamento:" in tag.text:
			data_fechamento = tag.text.replace("Fechamento:", "").strip()
			break

	dados = []
	for row in tabela.find_all("tr"):
		colunas = row.find_all("td")
		if len(colunas) < 2:
			continue

		municipio = colunas[0].text.strip()
		preco_txt = colunas[1].text.strip()

		if not municipio or preco_txt in ["-", ""]:
			continue

		texto_preco = preco_txt.replace(".", "").replace(",", ".").strip()
		try:
			dados.append({
				"fonte": "NoticiasAgricolas",
				"produto": "Arabica Duro",
				"municipio": municipio,
				"data": data_fechamento,
				"hora": "-",
				"preco": float(texto_preco)
			})
		except ValueError:
			continue

	return dados


# =========================================================
# MÉDIA POR PRODUTO
# =========================================================
def calcular_medias_por_produto(dados):
	medias = {}
	for d in dados:
		produto = d["produto"]
		if produto not in medias:
			medias[produto] = []
		medias[produto].append(d["preco"])

	return {produto: sum(precos) / len(precos) for produto, precos in medias.items()}


# =========================================================
# HANDLER VERCEL
# =========================================================
class handler(BaseHTTPRequestHandler):
	def do_GET(self):
		todos_dados = []
		todos_dados += scrape_cooabriel()
		todos_dados += scrape_cccv()
		todos_dados += scrape_noticias_agricolas_arabica_duro()

		medias = calcular_medias_por_produto(todos_dados)

		resultado = {
			"gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
			"cotacoes": todos_dados,
			"medias": medias
		}

		body = json.dumps(resultado, ensure_ascii=False).encode("utf-8")

		self.send_response(200)
		self.send_header("Content-Type", "application/json")
		self.end_headers()
		self.wfile.write(body)
