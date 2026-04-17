# ClubeETL.Worker

Worker ETL mÃ­nimo para ingestÃ£o de planilhas locais em tabelas de staging do ClubeBeneficios.

## Escopo
- localizar arquivos em pasta local
- ler planilhas XLSX
- processar hotel na aba `AGENDA 2026`
- processar creche em abas mensais
- gravar batches, runs, rows e erros operacionais
- mover arquivos para `processed` ou `error`

## Fora de escopo
- matching
- facts
- loyalty
- elegibilidade
- regras de negÃ³cio derivadas

## ExecuÃ§Ã£o
1. ajustar `appsettings.json`
2. colocar arquivos em `C:\ClubeETL\data\input`
3. executar o worker

## Modo
- `Manual`: roda uma vez e encerra
- `Watch`: fica monitorando a pasta