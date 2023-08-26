 with ftm_value as(
 SELECT   --ftm
        'FTM' as chain,
        fan.call_tx_hash as tx,   
        usd_add,
        sender,
        case
            when usd_add=0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590 --STG
                then cast(value as double) / 1e18 * 0.84
            else cast(value as double) / 1e6
        end as token_amount
    FROM layerzero_fantom_endpoint_fantom.Endpoint_call_send fan
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        from erc20_fantom.evt_Transfer
        
    ) fant ON fan.call_tx_hash = evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
    and (usd_add=0x04068DA6C83AFCFA0e13ba15A6696662335D5B75 --USDC
    or usd_add=0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590)  --STG
  
    
    UNION ALL
    
    SELECT  --avax
        'AVAX' as chain,
         ava.call_tx_hash as tx,
         usd_add,
        sender,
        case
            when usd_add=0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590 --STG
                then cast(value as double) / 1e18 * 0.84
            when usd_add=0xD24C2Ad096400B6FBcd2ad8B24E7acBc21A1da64 --FRAX
                then cast(value as double) / 1e18
            when usd_add=0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e --MAI
                then cast(value as double) / 1e18
            when usd_add=0xabc9547b534519ff73921b1fba6e672b5f58d083 --WOO
                then cast(value as double) / 1e18 * 0.18
            else cast(value as double) / 1e6
        end as token_amount
    FROM layerzero_avalanche_c.Endpoint_call_send ava
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_avalanche_c.evt_Transfer
    ) avat ON ava.call_tx_hash = avat.evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
        and (usd_add=0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7 --USDT
        or usd_add=0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E  --USDC
        or usd_add=0xD24C2Ad096400B6FBcd2ad8B24E7acBc21A1da64  --FRAX
        or usd_add=0xabc9547b534519ff73921b1fba6e672b5f58d083 --WOO
        or usd_add=0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e --MAI
        or usd_add=0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590)  --STG
         -- JOE
    
    
    UNION ALL
    
    SELECT  --BNB
        'BNB' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        case
            when usd_add=0xabc9547b534519ff73921b1fba6e672b5f58d083 --WOO
                then cast(value as double) / 1e18 * 0.18
            when usd_add=0xb0d502e938ed5f4df2e681fe6e419ff29631d62b --STG
                then cast(value as double) / 1e18 * 0.84
            when usd_add=0x3f56e0c36d275367b8c502090edf38289b3dea0d --MAI
                then cast(value as double) / 1e18
            else cast(value as double) / 1e18
        end as token_amount
        
    FROM layerzero_bnb.Endpoint_call_send bnb
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_bnb.evt_Transfer
    ) bnbt ON bnb.call_tx_hash = bnbt.evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
    AND (usd_add=0x55d398326f99059fF775485246999027B3197955 --USDT
    OR usd_add=0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56   --BUSD
    or usd_add=0xd17479997f34dd9156deef8f95a52d81d265be9c --USDD 
    or usd_add=0x4691937a7508860f876c9c0a2a617e7d9e945d4b --WOO
    or usd_add=0x3f56e0c36d275367b8c502090edf38289b3dea0d --MAI
    or usd_add=0xb0d502e938ed5f4df2e681fe6e419ff29631d62b) --STG
    --  METIS  JOE BTCB RDNT
   
    UNION ALL
    
    SELECT  --MATIC
        'MATIC' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        case
            when usd_add=0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063 --DAI
                then cast(value as double) / 1e18
            when usd_add=0xa3fa99a148fa48d14ed51d610c367c61876997f1 --MAI
                then cast(value as double) / 1e18
            when usd_add=0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590 --STG
                then cast(value as double) / 1e18 * 0.84
            else cast(value as double) / 1e6
        end as token_amount
    FROM layerzero_polygon.Endpoint_call_send matic
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_polygon.evt_Transfer
    ) matict ON matic.call_tx_hash = matict.evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
    AND (usd_add=0xc2132D05D31c914a87C6611C10748AEb04B58e8F --USDT
    OR usd_add=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 --USDC
    or usd_add=0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063  --DAI
    or usd_add=0x1b815d120b3ef02039ee11dc2d33de7aa4a8c603 --WOO
    or usd_add=0xa3fa99a148fa48d14ed51d610c367c61876997f1 --MAI
    or usd_add=0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590) --stg 
    --  BTCB
   
   UNION ALL
    
    SELECT  --op
        'OP' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        case
            when usd_add=0xc40f949f8a4e094d1b49a23ea9241d289b7b2819 --LUSC
                then cast(value as double) / 1e18
            when usd_add=0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9 --SUSD
                then cast(value as double) / 1e18
            when usd_add=0xda10009cbd5d07dd0cecc66161fc93d7c9000da1 --DAI
                then cast(value as double) / 1e18
            when usd_add=0xdfa46478f9e5ea86d57387849598dbfb2e964b02 --MAI
                then cast(value as double) / 1e18
            when usd_add=0x2e3d870790dc77a83dd1d18184acc7439a53f475 --FRAX
                then cast(value as double) / 1e18
            when usd_add=0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97 --STG
                then cast(value as double) / 1e18 * 0.84
            else cast(value as double) / 1e6
        end as token_amount
        
    FROM layerzero_optimism.Endpoint_call_send op
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_optimism.evt_Transfer
    ) opt ON op.call_tx_hash = opt.evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
    AND (usd_add=0xda10009cbd5d07dd0cecc66161fc93d7c9000da1 --DAI
    OR usd_add=0x7f5c764cbc14f9669b88837ca1490cca17c31607 --USDC
    or usd_add= 0x2e3d870790dc77a83dd1d18184acc7439a53f475 --FRAX
    or usd_add=0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9 --SUSD
    or usd_add=0xc40f949f8a4e094d1b49a23ea9241d289b7b2819 --LUSC
    or usd_add=0xdfa46478f9e5ea86d57387849598dbfb2e964b02 --MAI
    or usd_add=0xabc9547b534519ff73921b1fba6e672b5f58d083 --WOO
    or usd_add=0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97) --stg 
    --  BTCB
  
    
     UNION ALL
    
    SELECT  --op-eth
        'OPETH' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        (cast(value as double) / 1e18)*1800 as token_amount
        
    FROM layerzero_optimism.Endpoint_call_send op
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_optimism.evt_Transfer
    ) opt ON op.call_tx_hash = opt.evt_tx_hash 
    WHERE call_success = true 
    AND usd_add=0xb69c8CBCD90A39D8D3d3ccf0a3E968511C3856A0 --OP_SGETH
   
   
   UNION ALL
    
    SELECT  --arb
        'ARB' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        case
            when usd_add=0x93b346b6bc2548da6a1e7d98e9a421b42541425b --LUSD
                then cast(value as double) / 1e18
            when usd_add=0xa970af1a584579b618be4d69ad6f73459d112f95 --SUSD
                then cast(value as double) / 1e18
            when usd_add=0x17fc002b466eec40dae837fc4be5c67993ddbd6f --FRAX
                then cast(value as double) / 1e18
            when usd_add=0x3f56e0c36d275367b8c502090edf38289b3dea0d --MAI
                then cast(value as double) / 1e18
            when usd_add=0x6694340fc020c5e6b96567843da2df01b2ce1eb6 --STG
                then cast(value as double) / 1e18 * 0.84
            else cast(value as double) / 1e6
        end as token_amount
    FROM layerzero_arbitrum.Endpoint_call_send arb
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_arbitrum.evt_Transfer
    ) arbt ON arb.call_tx_hash = arbt.evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
    AND (usd_add=0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9 --USDT
    OR usd_add=0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8  --USDC
    OR usd_add=0xa970af1a584579b618be4d69ad6f73459d112f95 --SUSD
    OR usd_add=0x17fc002b466eec40dae837fc4be5c67993ddbd6f --FRAX
    or usd_add=0x93b346b6bc2548da6a1e7d98e9a421b42541425b --LUSD
    or usd_add=0xcafcd85d8ca7ad1e1c6f82f651fa15e33aefd07b --WOO
    or usd_add=0x3f56e0c36d275367b8c502090edf38289b3dea0d --MAI
    or usd_add=0x6694340fc020c5e6b96567843da2df01b2ce1eb6) --stg 
    -- JOE  BTCB RDNT
    
    
    UNION ALL  
    
    SELECT  --arb-eth
        'ARBETH' as chain,
        call_tx_hash as tx ,
        usd_add,
        sender,
        (cast(value as double) / 1e18)*1800 as token_amount
        
    FROM layerzero_arbitrum.Endpoint_call_send arb
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_arbitrum.evt_Transfer
    ) arbt ON arb.call_tx_hash = arbt.evt_tx_hash 
    WHERE call_success = true 
    AND usd_add=0x82CbeCF39bEe528B5476FE6d1550af59a9dB6Fc0 --ARB_SGETH
    
    
    
   UNION ALL
    
    SELECT  --ETH
        'ETH' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        case
            when usd_add=0x5f98805a4e8be255a32880fdec7f6728c6568ba0 --LUSD
                then cast(value as double) / 1e18
            when usd_add=0x57ab1ec28d129707052df4df418d58a2d46d5f51 --SUSD
                then cast(value as double) / 1e18
            when usd_add=0x853d955acef822db058eb8505911ed77f175b99e --FRAX
                then cast(value as double) / 1e18
            when usd_add=0x6b175474e89094c44da98b954eedeac495271d0f --DAI
                then cast(value as double) / 1e18
            when usd_add=0x8d6cebd76f18e1558d4db88138e2defb3909fad6 --MAI
                then cast(value as double) / 1e18
            when usd_add=0xaf5191b0de278c7286d6c7cc6ab6bb8a73ba2cd6 --STG
                then cast(value as double) / 1e18 * 0.84
            else cast(value as double) / 1e6
        end as token_amount
        
    FROM layerzero_ethereum.Endpoint_call_send eth
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_ethereum.evt_Transfer
    ) etht ON eth.call_tx_hash = etht.evt_tx_hash and _refundAddress=sender
    WHERE call_success = true 
    AND (usd_add=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 --USDC
    OR usd_add=0xdac17f958d2ee523a2206206994597c13d831ec7 --USDT
    or usd_add=0x6b175474e89094c44da98b954eedeac495271d0f --DAI
    or usd_add=0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6 --USDD
    or usd_add = 0x853d955acef822db058eb8505911ed77f175b99e --FRAX
    or usd_add =0x57ab1ec28d129707052df4df418d58a2d46d5f51 --SUSD
    or usd_add =0x5f98805a4e8be255a32880fdec7f6728c6568ba0  --LUSD
    or usd_add =0x8d6cebd76f18e1558d4db88138e2defb3909fad6 --MAI
    OR usd_add=0x4691937a7508860f876c9c0a2a617e7d9e945d4b --WOO
    or usd_add=0xaf5191b0de278c7286d6c7cc6ab6bb8a73ba2cd6) --stg 
    --METIS  BTCB
   UNION ALL  
    
    SELECT  --ETH-eth
        'ETHeth' as chain,
        call_tx_hash as tx,
        usd_add,
        sender,
        (cast(value as double) / 1e18)*1800 as token_amount
        
    FROM layerzero_ethereum.Endpoint_call_send eth
    LEFT JOIN (
        SELECT contract_address as usd_add ,value, "from" as sender ,evt_tx_hash
        FROM erc20_ethereum.evt_Transfer
    ) etht ON eth.call_tx_hash = etht.evt_tx_hash 
    WHERE call_success = true
    AND usd_add=0x72E2F4830b9E45d52F80aC08CB2bEC0FeF72eD9c --ETH_SGETH
)

select sender,sum(token_amount) as totalUSD
from ftm_value
group by sender
order by totalUSD desc
