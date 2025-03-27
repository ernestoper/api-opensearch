from fastapi import FastAPI, Query, HTTPException, Depends
from opensearchpy import OpenSearch
from datetime import datetime
from typing import Optional, List, Dict, Any
import configparser
from pathlib import Path
from dotenv import load_dotenv
import os
import logging
from pydantic import BaseModel, validator

# Adicionar importação para o pyngrok
try:
    from pyngrok import ngrok
    ngrok_available = True
except ImportError:
    ngrok_available = False
    print("Ngrok não está instalado. Execute 'pip install pyngrok' para habilitar o túnel Ngrok.")

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI(
    title="Deviations API",
    description="""
API para consultar desvios de câmeras armazenados no OpenSearch:

*   Esta API permite filtrar desvios por nome da câmera, tipo de evento, tipo de câmera e intervalo de tempo. 
    Também inclui endpoints para verificar a saúde da API e consultar desvios específicos pelo ID
* Como usar:
    - Acesse os endpoints abaixo para realizar consultas.
    - Use os filtros opcionais para refinar os resultados.
    - Para testar a API, utilize os exemplos fornecidos na documentação.
* Links Úteis:
    - [Documentação Interativa](/docs)
    - [Documentação OpenAPI](/openapi.json)

* Endpoints Disponíveis:
    - **GET /deviations**: Consulta desvios de câmeras com filtros opcionais.
    - **GET /deviations/{deviation_id}**: Consulta detalhes de um desvio específico pelo ID.
    - **GET /health**: Verifica o status da API e do cluster OpenSearch.

*  Exemplos de Uso:
    - **Filtrar desvios por nome da câmera:**
      ```bash
      GET /deviations?camera_name=Camera_01
      ```
    - **Filtrar desvios por intervalo de tempo:**
      ```bash
      GET /deviations?start_time=2023-10-01T00:00:00&end_time=2023-10-01T23:59:59
      ```
    - **Consultar desvio específico pelo ID:**
      ```bash
      GET /deviations/12345
      ```
    - **Verificar saúde da API:**
      ```bash
      GET /health
      ```

* Respostas Esperadas:
    - **Lista de desvios:**
      ```json
      {
        "deviations": [
          {
            "camera_name": "Camera_01",
            "event_type": "motion_detected",
            "timestamp": "2023-10-01T12:34:56"
          }
        ]
      }
      ```
    - **Detalhes de um desvio:**
      ```json
      {
        "camera_name": "camera_01",
        "event_type": "motion_detected",
        "timestamp": "2023-10-01T12:34:56"
      }
      ```
    - **Status de saúde da API:**
      ```json
      {
        "status": "healthy",
        "opensearch_status": "green",
        "version": "1.0.0"
      }
      ```

* Códigos de Resposta:
    - **200**: Sucesso na requisição.
    - **400**: Parâmetros inválidos.
    - **404**: Desvio não encontrado.
    - **500**: Erro ao consultar o OpenSearch.
    - **503**: Serviço indisponível.

* Configuração:
    - **OpenSearch**: Certifique-se de que o arquivo `config.ini` e o arquivo `.env` estão configurados corretamente.
    - **Ngrok**: Opcional para expor a API externamente. Instale com `pip install pyngrok`.

* Observações:
    - A API utiliza autenticação básica para acessar o OpenSearch.
    - O uso de Ngrok é opcional e pode ser configurado com um token de autenticação.
    """,
    version="1.0.0"
)

# Paths to configuration files
config_file = Path("config.ini")
env_file = Path(".env")

# Check if configuration files exist
if not config_file.exists():
    raise FileNotFoundError("Arquivo config.ini não encontrado!")
if not env_file.exists():
    raise FileNotFoundError("Arquivo .env não encontrado!")

# Load environment variables from .env
load_dotenv()

# Read config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# OpenSearch configuration
def get_opensearch_client():
    client = OpenSearch(
        hosts=[{"host": config["opensearch"]["host"], "port": int(config["opensearch"]["port"])}],
        http_auth=(os.getenv("OPENSEARCH_USERNAME"), os.getenv("OPENSEARCH_PASSWORD")),
        use_ssl=config["opensearch"].getboolean("use_ssl"),
        verify_certs=config["opensearch"].getboolean("verify_certs")
    )
    
    # Verify OpenSearch connection
    if not client.ping():
        raise Exception("Não foi possível conectar ao OpenSearch!")
    
    return client

# Modelo para validação dos parâmetros de consulta
class DeviationQueryParams(BaseModel):
    camera_name: Optional[str] = None
    event_type: Optional[str] = None
    camera_type: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    size: int = 10
    from_: int = 0

    @validator("start_time", "end_time", pre=True)
    def parse_datetime(cls, value):
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                raise ValueError("Formato de data inválido. Use o formato ISO 8601 (YYYY-MM-DDTHH:MM:SS).")
        return value

# Endpoint to query deviations
@app.get(
    "/deviations",
    response_model=Dict[str, List[Dict[str, Any]]],
    summary="Consultar desvios de câmeras",
    description="""
    **Consulta desvios de câmeras com filtros opcionais.**

    Este endpoint permite buscar desvios armazenados no OpenSearch. Você pode aplicar filtros como nome da câmera, tipo de evento, intervalo de tempo e mais.

    ### Exemplo de Uso:
    - Filtrar por nome da câmera: `?camera_name=Camera_01`
    - Filtrar por intervalo de tempo: `?start_time=2023-10-01T00:00:00&end_time=2023-10-01T23:59:59`
    - Paginação: `?size=20&from=10`

    ### Resposta:
    ```json
    {
      "deviations": [
        {
          "camera_name": "Camera_01",
          "event_type": "motion_detected",
          "timestamp": "2023-10-01T12:34:56"
        }
      ]
    }
    ```
    """,
    responses={
        200: {"description": "Lista de desvios encontrados"},
        400: {"description": "Parâmetros inválidos"},
        500: {"description": "Erro ao consultar o OpenSearch"}
    }
)
def get_deviations(
    camera_name: Optional[str] = Query(None, description="Filtrar por nome da câmera"),
    event_type: Optional[str] = Query(None, description="Filtrar por tipo de evento"),
    camera_type: Optional[str] = Query(None, description="Filtrar por tipo de câmera"),
    start_time: Optional[datetime] = Query(None, description="Início do intervalo de tempo"),
    end_time: Optional[datetime] = Query(None, description="Fim do intervalo de tempo"),
    size: int = Query(10, description="Número de resultados por página", ge=1, le=100),
    from_: int = Query(0, alias="from", description="Iniciar a partir do resultado"),
    opensearch_client: OpenSearch = Depends(get_opensearch_client)
):
    try:
        # Validação dos parâmetros
        query_params = DeviationQueryParams(
            camera_name=camera_name,
            event_type=event_type,
            camera_type=camera_type,
            start_time=start_time,
            end_time=end_time,
            size=size,
            from_=from_
        )

        # Build the OpenSearch query based on filters
        query = {"query": {"bool": {"must": []}}}
        
        # Add filters to the query
        if query_params.camera_name:
            query["query"]["bool"]["must"].append({"term": {"camera_name": query_params.camera_name}})
        
        if query_params.event_type:
            query["query"]["bool"]["must"].append({"term": {"event_type": query_params.event_type}})
            
        if query_params.camera_type:
            query["query"]["bool"]["must"].append({"term": {"camera_type": query_params.camera_type}})
        
        # Add time range filter if both start and end times are provided
        if query_params.start_time and query_params.end_time:
            if query_params.start_time > query_params.end_time:
                raise HTTPException(status_code=400, detail="start_time deve ser anterior a end_time")
                
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "gte": query_params.start_time.isoformat(),
                        "lte": query_params.end_time.isoformat()
                    }
                }
            })
        elif query_params.start_time:
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "gte": query_params.start_time.isoformat()
                    }
                }
            })
        elif query_params.end_time:
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "lte": query_params.end_time.isoformat()
                    }
                }
            })
        
        # Execute the query in OpenSearch
        response = opensearch_client.search(
            index="deviations",
            body=query,
            size=query_params.size,
            from_=query_params.from_
        )
        
        # Return the results
        deviations = [hit["_source"] for hit in response["hits"]["hits"]]
        return {"deviations": deviations}
    
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erro ao consultar OpenSearch: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar OpenSearch: {str(e)}")

# Endpoint to query a deviation by ID
@app.get("/deviations/{deviation_id}", 
        response_model=Dict[str, Any],
        summary="Consultar desvio específico pelo ID",
        description="""
        **Consulta detalhes de um desvio específico pelo ID.**

        Este endpoint retorna informações detalhadas sobre um desvio específico armazenado no OpenSearch.

        ### Exemplo de Uso:
        - Buscar desvio pelo ID: `/deviations/12345`

        ### Resposta:
        ```json
        {
        "camera_name": "camera_01",
        "event_type": "motion_detected",
        "timestamp": "2023-10-01T12:34:56"
        }
        ```
        """)
def get_deviation_by_id(
    deviation_id: str,
    opensearch_client: OpenSearch = Depends(get_opensearch_client)
):
    try:
        response = opensearch_client.get(
            index="deviations",
            id=deviation_id
        )
        return response["_source"]
    except Exception as e:
        logging.error(f"Desvio não encontrado: {str(e)}")
        raise HTTPException(status_code=404, detail=f"Desvio não encontrado: {str(e)}")

# Endpoint to check the health of the API
@app.get(   "/health",
            summary="Verificar saúde da API",
            description="""
            **Verifica o status da API e do cluster OpenSearch.**

            Este endpoint é útil para monitorar a disponibilidade da API e do banco de dados.

            ### Exemplo de Resposta:
            ```json
            {
            "status": "healthy",
            "opensearch_status": "green",
            "version": "1.0.0"
            }
            ```
            """)
def health_check(opensearch_client: OpenSearch = Depends(get_opensearch_client)):
    try:
        health = opensearch_client.cluster.health()
        return {
            "status": "healthy",
            "opensearch_status": health["status"],
            "version": app.version
        }
    except Exception as e:
        logging.error(f"Serviço indisponível: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Serviço indisponível: {str(e)}")

# Configuração do túnel Ngrok
def setup_ngrok(port):
    if not ngrok_available:
        logging.warning("Ngrok não está disponível. Certifique-se de que o pyngrok está instalado.")
        return None
    
    try:
        # Obter token do Ngrok do .env (opcional)
        ngrok_token = os.getenv("NGROK_AUTH_TOKEN")
        if ngrok_token:
            ngrok.set_auth_token(ngrok_token)
        
        # Iniciar um túnel HTTP para a porta especificada usando o domínio fixo
        public_url = ngrok.connect(port, proto="http", domain="elephant-moved-informally.ngrok-free.app")
        logging.info(f"Ngrok tunnel ativo! URL pública: {public_url}")
        return public_url
    except Exception as e:
        logging.error(f"Erro ao iniciar o Ngrok: {str(e)}")
        return None

# Start the FastAPI server
if __name__ == "__main__":
    import uvicorn
    
    # Configuração do servidor
    host = "0.0.0.0"
    port = 8000
    
    # Configurar Ngrok se estiver disponível
    ngrok_url = None
    if ngrok_available:
        ngrok_url = setup_ngrok(port)
        if ngrok_url:
            logging.info(f"API disponível externamente em: {ngrok_url}")
            logging.info(f"Documentação disponível em: {ngrok_url}/docs")
        else:
            logging.warning("Falha ao iniciar o Ngrok. A API só estará disponível localmente.")
    
    # Iniciar o servidor
    logging.info(f"Servidor iniciando em: http://{host}:{port}")
    logging.info(f"Documentação disponível em: http://{host}:{port}/docs")
    uvicorn.run(app, host=host, port=port)
