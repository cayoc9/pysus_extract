# Manual da API PySUS Extract

## Visão Geral
API para extração e consulta de dados do DataSUS, com suporte para:
- SIH (Sistema de Informações Hospitalares)
- SIA (Sistema de Informações Ambulatoriais)

## Endpoint Principal

### POST /query
**URL Base**: `http://node208714-pysus.sp1.br.saveincloud.net.br:15473/query`

### Estrutura da Requisição
```json
{
    "base": "SIH",                                    // Base de dados (SIH ou SIA)
    "grupo": "RD",                                    // Código do grupo (ver lista abaixo)
    "cnes_list": ["2077485", "2077493"],             // Lista de códigos CNES
    "campos_agrupamento": ["CNES", "ANO_CMPT", "MES_CMPT"], // Campos para agrupamento
    "competencia_inicio": "01/2022",                  // Formato: MM/YYYY
    "competencia_fim": "12/2022"                      // Formato: MM/YYYY
}
```

### Grupos Disponíveis por Base

#### SIH (Sistema de Informações Hospitalares)
- **RD**: AIH Reduzida
- **RJ**: AIH Rejeitada
- **ER**: AIH Rejeitada com erro
- **SP**: Serviços Profissionais

#### SIA (Sistema de Informações Ambulatoriais)
- **PA**: Produção Ambulatorial
- **BI**: Boletim de Produção Ambulatorial Individualizado
- **AB**: APAC de Cirurgia Bariátrica
- **ABO**: APAC de Acompanhamento Pós Cirurgia Bariátrica
- **ACF**: APAC de Confecção de Fístula
- **AD**: APAC de Laudos Diversos
- **AM**: APAC de Medicamentos
- **AMP**: APAC de Acompanhamento Multiprofissional
- **AN**: APAC de Nefrologia
- **AQ**: APAC de Quimioterapia
- **AR**: APAC de Radioterapia
- **ATD**: APAC de Tratamento Dialítico

#### Outros Grupos
- **PS**: RAAS Psicossocial
- **SAD**: RAAS de Atenção Domiciliar
- **CH**: Cadastro Hospitalar
- **DC**: Dados Complementares
- **EE**: Estabelecimento de Ensino
- **EF**: Estabelecimento Filantrópico
- **EP**: Equipes
- **EQ**: Equipamentos
- **GM**: Gestão Metas
- **HB**: Habilitação
- **IN**: Incentivos
- **LT**: Leitos
- **PF**: Profissional
- **RC**: Regra Contratual
- **SR**: Serviço Especializado
- **ST**: Estabelecimentos

### Campos CNES por Grupo
```json
{
    "AB": "ap_cnspcn",
    "ABO": "ap_cnspcn",
    "ACF": "ap_cnspcn",
    "AD": "ap_cnspcn",
    "AM": "ap_cnspcn",
    "AMP": "ap_cnspcn",
    "AN": "ap_cnspcn",
    "AQ": "ap_cnspcn",
    "AR": "ap_cnspcn",
    "ATD": "ap_cnspcn",
    "BI": ["cns_pac", "cnsprof"],
    "PA": "PA_CODUNI",
    "RD": "CNES",
    "RJ": "cnes",
    "ER": "CNES",
    "SP": "sp_cnes"
}
```

### Resposta de Sucesso
```json
{
    "status": "success",
    "message": "Consulta processada com sucesso",
    "dados": [...],  // Array com os registros encontrados
    "total_registros": 1234,
    "colunas": ["CNES", "ANO_CMPT", "MES_CMPT"],
    "table_name": "sih_aih_reduzida"
}
```

### Códigos de Erro
- **400**: Parâmetros inválidos
- **404**: Dados não encontrados
- **500**: Erro interno do servidor

### Exemplo de Implementação em JavaScript
```javascript
const fetchDataSUS = async (params) => {
  try {
    const response = await fetch(
      'http://node208714-pysus.sp1.br.saveincloud.net.br:15473/query',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(params)
      }
    );

    if (!response.ok) {
      throw new Error(`Erro na requisição: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Erro ao buscar dados:', error);
    throw error;
  }
};
```

### Processamento e Armazenamento
1. Os arquivos são buscados em formato Parquet
2. Processamento usando DuckDB para melhor performance
3. Resultados são salvos automaticamente:
   - No PostgreSQL (sempre)
   - Em CSV (se menos de 10M linhas)

### Validações
1. **Base de dados**: Deve ser "SIH" ou "SIA"
2. **Grupo**: Deve ser um dos códigos válidos listados acima
3. **Competências**: Formato MM/YYYY
4. **CNES**: Lista não vazia de códigos CNES
5. **Campos de agrupamento**: Lista de colunas para agregação

### Logs e Monitoramento
- **Logs da API**: `logs/app.log`
- **Arquivos CSV**: `consultas/[nome_tabela]_[timestamp].csv`

### Documentação Interativa
Swagger UI disponível em: `http://node208714-pysus.sp1.br.saveincloud.net.br:15473/docs`

### Observações Importantes
1. CORS está habilitado para todas as origens (`*`)
2. O campo CNES é automaticamente incluído nos campos de agrupamento
3. Os nomes das tabelas seguem o padrão: `[base]_[grupo]` em lowercase
4. Suporte para todos os estados brasileiros
5. Dados são processados mês a mês dentro do período especificado
