n  # Plano de Implementação API DataSUS

## 1. Estrutura Atual
Já existem implementados:
- Modelo SIH_RD (AIH Reduzida)
- Modelo SIH_RJ (AIH Rejeitada)
- Endpoint GET /query básico
- Estrutura de arquivos parquet

## 2. Grupos a Implementar
Ordem de prioridade:

### Fase 1 - Grupos SIH
1. SIH_ER (AIH Rejeitada com erro)
   - Modelo SQLAlchemy
   - Migração da tabela
   - Validações específicas
   - Processamento parquet

### Fase 2 - Serviços
2. SIH_SP (Serviços Profissionais)
   - Modelo SQLAlchemy
   - Migração da tabela
   - Validações específicas
   - Processamento parquet

### Fase 3 - Ambulatorial 
3. SIA_PA (Produção Ambulatorial)
   - Modelo SQLAlchemy
   - Migração da tabela
   - Validações específicas
   - Processamento parquet

## 3. Implementação por Grupo

### 3.1 Modelo SIH_ER
```python
class SIH_ER(Base):
    __tablename__ = 'sih_aih_rejeitada_erro'
    
    # Campos padrão SIH
    id = Column(Integer, primary_key=True, autoincrement=True)
    UF_ZI = Column(String(2))
    ANO_CMPT = Column(Integer)
    MES_CMPT = Column(Integer)
    
    # Campos específicos de erro
    MOTIVO_REJ = Column(String(255))
    ERRO_SMS = Column(String(255))
    ERRO_SES = Column(String(255)) 
    ERRO_CRIT = Column(String(255))
    NUM_CRIT = Column(Integer)
    
    # Campos de valores
    VAL_APRES = Column(Float)
    VAL_GLOSA = Column(Float)
```

### 3.2 Modelo SIH_SP
```python
class SIH_SP(Base):
    __tablename__ = 'sih_servicos_profissionais'
    
    # Campos padrão
    id = Column(Integer, primary_key=True)
    UF_ZI = Column(String(2))
    ANO_CMPT = Column(Integer)
    MES_CMPT = Column(Integer)
    
    # Campos específicos SP
    PROC_REA = Column(String(50))
    PROF_EXE = Column(String(50))
    CBO = Column(String(10))
    SERVICO = Column(String(50))
    
    # Campos de valores
    VAL_TOTAL = Column(Float)
```

## 4. Modificações API

### 4.1 Endpoint Query
Adicionar suporte para novos grupos:
```python
@app.get("/query")
async def query_data(
    base: str,  # SIH, SIA
    grupo: str,  # RD, RJ, ER, SP, PA
    estados: List[str],
    colunas: List[str],
    competencia_inicio: str,
    competencia_fim: str
):
    # Validar grupo
    if grupo not in ['RD', 'RJ', 'ER', 'SP', 'PA']:
        raise HTTPException(...)
```

### 4.2 Validações por Grupo
```python
def validate_group_columns(grupo: str, colunas: List[str]):
    valid_columns = {
        'ER': ['MOTIVO_REJ', 'ERRO_SMS', ...],
        'SP': ['PROC_REA', 'PROF_EXE', ...],
        'PA': ['PROC_AMB', 'QT_PROC', ...]
    }
```

## 5. Estrutura de Arquivos Parquet

### SIH
```
/parquet_files/SIH/
  /RD/  # AIH Reduzida
  /RJ/  # AIH Rejeitada
  /ER/  # AIH Rejeitada com erro
  /SP/  # Serviços Profissionais
```

### SIA 
```
/parquet_files/SIA/
  /PA/  # Produção Ambulatorial
```

## 6. Monitoramento e Logs

- Logging por grupo de dados
- Métricas de performance
- Controle de erros específicos
- Validação de dados

## 7. Testes

- Testes unitários por modelo
- Testes de integração por grupo
- Testes de carga
- Validação de dados