#!/bin/bash

# Script para listar issues abertas de um repositório do GitHub
# Uso: ./list_issues.sh OWNER REPO
# Exemplo: ./list_issues.sh christianomatos planeta-ia

# Verifica se o token do GitHub está definido
if [ -z "$GITHUB_TOKEN" ]; then
    echo "Erro: A variável de ambiente GITHUB_TOKEN não está definida."
    echo "Por favor, defina o token do GitHub para usar este script:"
    echo "  export GITHUB_TOKEN=$GITHUB_TOKEN"
    exit 1
fi

# Verifica se os parâmetros foram fornecidos
if [ $# -ne 2 ]; then
    echo "Uso: $0 OWNER REPO"
    echo "Exemplo: $0 christianomatos planeta-ia"
    exit 1
fi

OWNER="$1"
REPO="$2"

# URL da API do GitHub para listar issues abertas
URL="https://api.github.com/repos/${OWNER}/${REPO}/issues?state=open"

# Faz a requisição à API do GitHub
response=$(curl -s -H "Authorization: token ${GITHUB_TOKEN}" "${URL}")

# Verifica se houve erro na requisição
if [ $? -ne 0 ]; then
    echo "Erro ao fazer requisição para a API do GitHub."
    exit 1
fi

# Verifica se a resposta contém uma mensagem de erro da API
if echo "$response" | grep -q '"message"'; then
    echo "Erro na API do GitHub:"
    echo "$response" | grep -o '"message": *"[^"]*"' | cut -d'"' -f4
    exit 1
fi

# Processa e exibe as issues usando Python para parsing JSON
echo "$response" | python3 -c "
import sys
import json

try:
    issues = json.load(sys.stdin)
    
    if not issues:
        print('Nenhuma issue aberta encontrada.')
        sys.exit(0)
    
    print(f'Issues abertas para {sys.argv[1]}/{sys.argv[2]}:')
    print('-' * 60)
    
    for issue in issues:
        number = issue.get('number', 'N/A')
        title = issue.get('title', 'Sem título')
        author = issue.get('user', {}).get('login', 'Desconhecido')
        html_url = issue.get('html_url', '')
        
        print(f'#{number} - {title}')
        print(f'  Autor: {author}')
        print(f'  URL: {html_url}')
        print('-' * 60)
    
    print(f'Total: {len(issues)} issue(s) aberta(s)')
    
except json.JSONDecodeError as e:
    print(f'Erro ao processar resposta JSON: {e}')
    print('Resposta recebida:')
    print(sys.stdin.read())
    sys.exit(1)
" - "$OWNER" "$REPO"
