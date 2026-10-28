---
project: "Importação de Dados do Mapa das Organizações da Sociedade Civil - IPEA"
language: "R"
---

# Project Context
Esse projeto serve para atualizar os dados do 'Mapa das Organizações da 
Sociedade Civil' nos servidores do IPEA (governo federal do Brasil)

# Tech Stack & Standards
* Use the `tidyverse` library for all data manipulation.
* O script @src/atualiza_dados_OSC.R coordena atualização. Através dele todos os 
demais scripts e funções são chamados, cada um com um objetivo específico, para 
completar a atualização.
* Sempre limpre da memória, usando a função 'rm', os objetos criados no script. 
Essa limpeza deve ser feita no final do código. Se já houver uma função rm para
aquele objeto, manter a função como está e não propor nova remoção de objeto.
* Nunca deixe que uma linha de código ou comentário tenha mais de 81 caracteres.
* Sempre carregue as bibliotecas necessárias para o código na seção # Setup ####.
* Desfaça as conexões a bancos de dados, usando a função 'dbDisconnect' (pacote 
DBI), sempre que não for mais necessário mais nenhuma requisição de dados ao 
banco. Essa desconexão deve ser feita no final do script.

# Coding Requirements

# Explicit Boundaries
