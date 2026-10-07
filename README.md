Hi, I’m Arun Vinayak Rathod, a software developer passionate about creating and solving real-world problems.

🚀 I'm currently working on projects using Spring Boot and Spring Security for the backend, and React.js for the frontend.

💡 I'm interested in collaborating on projects involving Java, React.js, Spring, and Hibernate.

📧 You can reach me at arunvrathod123@gmail.com to discuss potential collaborations or opportunities.

Let's connect and build something amazing together!

💻Language and Tools 🛠

![Spring](https://img.shields.io/badge/spring-%236DB33F.svg?style=for-the-badge&logo=spring&logoColor=white) ![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=openjdk&logoColor=white) ![JavaScript](https://img.shields.io/badge/javascript-%23323330.svg?style=for-the-badge&logo=javascript&logoColor=%23F7DF1E) ![Hibernate](https://img.shields.io/badge/Hibernate-59666C?style=for-the-badge&logo=Hibernate&logoColor=white) ![React](https://img.shields.io/badge/react-%2320232a.svg?style=for-the-badge&logo=react&logoColor=%2361DAFB) ![Redux](https://img.shields.io/badge/redux-%23593d88.svg?style=for-the-badge&logo=redux&logoColor=white) ![MySQL](https://img.shields.io/badge/mysql-4479A1.svg?style=for-the-badge&logo=mysql&logoColor=white) ![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white) ![HTML5](https://img.shields.io/badge/html5-%23E34F26.svg?style=for-the-badge&logo=html5&logoColor=white) ![TailwindCSS](https://img.shields.io/badge/tailwindcss-%2338B2AC.svg?style=for-the-badge&logo=tailwind-css&logoColor=white) ![CSS3](https://img.shields.io/badge/css3-%231572B6.svg?style=for-the-badge&logo=css3&logoColor=white) ![C++](https://img.shields.io/badge/c++-%2300599C.svg?style=for-the-badge&logo=c%2B%2B&logoColor=white) ![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white) ![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white) ![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black) 





@PostMapping("/allocation")
    @ResponseStatus(HttpStatus.CREATED)
    public AggregateDemandResponse allocateReservation(@Parameter(hidden = true) @RequestHeader(name = AUTHORIZATION, required = false) String authorization,
                                                       @Parameter(hidden = true) @RequestAttribute Map<String, String> tracingHeaders,
                                                       @Valid @RequestBody AggregateDemandRequest aggregateDemandAllocation) {
        try {
            MDC.setContextMap(tracingHeaders);
            Validator.validateAggregateDemand(aggregateDemandAllocation);
            List<DemandTransaction> savedDemand = demandServiceWrapper.allocateDemand(aggregateDemandAllocation);
            return responseCreator
                    .buildAggregateDemandResponseFromDemandTransactionForAllocation(aggregateDemandAllocation.getReferenceId(), savedDemand);
        } finally {
            MDC.clear();
        }
    }    public List<DemandTransaction> allocateDemand(AggregateDemandRequest aggregateDemandRequest) {
        // No try/catch here — exceptions propagate to ErrorHandler which logs with full stack trace.
        log.debug("Inside DemandServiceWrapper : allocateDemand()");
        List<DemandTransaction> allocatedDemandTransactionList = new ArrayList<>();
        List<DemandTransaction> demandTransactionList = new ArrayList<>();
        Map<DemandKey, ATP> allocationAtpMap = new HashMap<>();
        Set<String> demandSkipNodeIds = getDemandSkipNodeIds();
        List<DemandTransaction> persistedAllocatedDemandTransactions = demandService.allocateDemandTransaction(aggregateDemandRequest,
                allocatedDemandTransactionList, allocationAtpMap, demandTransactionList, true,
                demandSkipNodeIds);
        Map<Boolean, List<DemandTransaction>> partitioned = demandTransactionList.stream()
                .collect(Collectors.partitioningBy(demandTransaction -> demandSkipNodeIds.contains(demandTransaction.getNodeId())));
        List<DemandTransaction> demandSkipNodeIdsTransactionList = partitioned.get(true);
        List<DemandTransaction> nonDemandSkipTransactionList = partitioned.get(false);
        DemandUpdateWrapper demandUpdateWrapper = DemandUpdateWrapper.builder()
                .demandTransactionEvent(demandTransactionList)
                .build();
        DemandUpdateWrapper filteredDemandUpdateWrapper = DemandUpdateWrapper.builder()
                .demandTransactionEvent(nonDemandSkipTransactionList)
                .build();
        if (!CollectionUtils.isEmpty(nonDemandSkipTransactionList)) {
            demandReconcileEventsPublisher.publishToInventoryDemandReconcileEvent(filteredDemandUpdateWrapper);
        }
        if (cacheService.isCacheEnabled()) {
            log.debug("Updating cache for demand allocation");
            if (CollectionUtils.isNotEmpty(demandSkipNodeIdsTransactionList)) {
                demandService.updateCacheForDemandSkipAllocation(demandSkipNodeIdsTransactionList);
            }
            if (CollectionUtils.isNotEmpty(persistedAllocatedDemandTransactions)) {
                demandService.updateCacheForDemandAllocation(persistedAllocatedDemandTransactions, allocationAtpMap, aggregateDemandRequest);
            }
        }
        publishInventoryListenerEvents(demandUpdateWrapper, true, MDC.getCopyOfContextMap());
        if (ATPMode.ALLOCATED.equals(availabilityATPMode)) {
            availabilityService.pushAvailabilityEventToPubSub(allocationAtpMap);
        }
        return persistedAllocatedDemandTransactions;
    }
  @Transactional(timeout = 7)
    public List<DemandTransaction> allocateDemandTransaction(AggregateDemandRequest aggregateDemandAllocation,
                                                             List<DemandTransaction> allocatedDemandTransactionList,
                                                             Map<DemandKey, ATP> allocationAtpMap,
                                                             List<DemandTransaction> demandTransactionList,
                                                             boolean shouldSkipAllocationForDemandSkipNodeIds,
                                                             Set<String> demandSkipNodeIds) {

        log.info("Received request for allocate demand: {}", aggregateDemandAllocation);
        allocatedDemandTransactionList.addAll(buildDemandTransactions(aggregateDemandAllocation, ALLOCATED,
                false, RESERVATION_ID));

        allocationAtpMap.putAll(atpService.getAllocationAtpWithLock(allocatedDemandTransactionList));

        List<ATP> unAvailableSkus = new ArrayList<>();
        List<String> outOfStockLogisticSkus = new ArrayList<>();
        allocatedDemandTransactionList.forEach(demandTransaction -> {
            DemandKey key = DemandKey.builder()
                    .logisticSkuId(demandTransaction.getLogisticSkuId())
                    .nodeId(demandTransaction.getNodeId())
                    .condition(demandTransaction.getCondition())
                    .build();

            ATP atp = allocationAtpMap.getOrDefault(key, new ATP());

            Quantity atpQuantity = atp.getQuantity();
            Quantity demandQuantity = new Quantity(demandTransaction.getQuantity(), demandTransaction.getQuantityUOM());
            if (!checkIfSufficientQuantity(demandQuantity, atpQuantity)) {
                unAvailableSkus.add(atp);
                outOfStockLogisticSkus.add(atp.getLogisticSkuId());
            } else {
                demandTransaction.setDemandType(ALLOCATED);
                if (!Objects.isNull(aggregateDemandAllocation.getOrderType())) {
                    demandTransaction.setOrderType(aggregateDemandAllocation.getOrderType());
                }
            }
        });
        if (!aggregateDemandAllocation.isForceAllocation()) {
            if (!unAvailableSkus.isEmpty()) {
                CompletableFuture.runAsync(() -> evictAndRepopulateCacheForUnAvailableSkusList(unAvailableSkus));
                log.error("Out of stock error occurred during Demand allocation with following skus being out of stock :: [{}] with nodeId :: [{}]",
                        outOfStockLogisticSkus, allocatedDemandTransactionList.get(0).getNodeId());
                List<ATP> unAvailableSkusResponse = nullifyReasonCodes(unAvailableSkus);
                throw OutOfStockException.itemsOutOfStock(unAvailableSkusResponse);
            }
        }
        if (shouldSkipAllocationForDemandSkipNodeIds) {
            allocatedDemandTransactionList.removeIf(demandTransaction -> demandSkipNodeIds.contains(demandTransaction.getNodeId()));
        }
        log.debug("saving demand list");
        List<DemandTransaction> demandListFromDB = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(allocatedDemandTransactionList)) {
            demandListFromDB = demandTransactionRepository.saveAll(allocatedDemandTransactionList);
        }
        List<DemandTransaction> reservedDemandTransactionList = buildDemandTransactions(aggregateDemandAllocation, RESERVED,
                true, RESERVATION_ID);
        demandTransactionRepository.saveAll(reservedDemandTransactionList);
        demandTransactionList.addAll(allocatedDemandTransactionList);
        demandTransactionList.addAll(reservedDemandTransactionList);
        updateSupplyForSellerDemandAllocation(reservedDemandTransactionList);
        log.debug("demand allocated for referenceId: {}", aggregateDemandAllocation.getReferenceId());
        return demandListFromDB;
    }.    public Map<DemandKey, ATP> getAllocationAtpWithLock(List<DemandTransaction> demandTransactionList) {
        log.debug("getAllocationAtpWithLock() started");
        List<DemandTransaction> demandTransactionSortedList = demandTransactionList.stream().sorted(Comparator.comparing(DemandTransaction::getLogisticSkuId))
                .collect(Collectors.toList());
        return demandTransactionSortedList.stream()
                .map(demandAllocation -> getNodeAtpForAllocationWithLock(
                        demandAllocation.getLogisticSkuId(),
                        (demandAllocation.getCondition() != null) ? demandAllocation.getCondition() : Condition.NEW,
                        demandAllocation.getNodeId())
                ).collect(Collectors.toMap(atp -> DemandKey.builder()
                                .nodeId(atp.getNodeId())
                                .logisticSkuId(atp.getLogisticSkuId())
                                .condition(atp.getCondition())
                                .build()
                        , atp -> atp, (atp1, atp2) -> atp1));
    }



     public ATP getNodeAtpForAllocationWithLock(String logisticSkuId, Condition condition, String nodeId) {
        log.debug("getNodeAtpForAllocationWithLock() started");
        ATPWrapper atpWrapper = atpdbStore.getSingleAllocatedATPWithLock(logisticSkuId, condition, nodeId);
        if (atpWrapper != null) {
            safetyStockService.enrichSafetyStock(List.of(atpWrapper));
            return enrichAtpWrapperWithATPAndThresholdValues(atpWrapper, ALLOCATED);
        }
        return getDefaultATP(nodeId, logisticSkuId, condition);
    } public ATPWrapper getSingleAllocatedATPWithLock(String logisticSkuId, Condition condition, String nodeId) {
        double demandCount = demandService.getAllocatedCountByNodeAndSkuWithLock(logisticSkuId, condition, nodeId);
        Optional<Supply> optionalSupply = supplyService.getAvailableSupplies(nodeId, logisticSkuId, condition);
        if (optionalSupply.isPresent()) {
            Supply supply = optionalSupply.get();
            // First demand count which represents Reserved + allocated demand can be taken as 0 as it is not being used
            // in the flow anywhere and to keep buildATPData method common for all flows
            return buildATPData(supply, 0D, demandCount);
        }
        return null;
    } public double getAllocatedCountByNodeAndSkuWithLock(String logisticSkuId, Condition condition, String nodeId) {
        log.debug("Get demand with lock for logisticsSkuId : {}, condition : {}, node-id : {} and demandType: {}", logisticSkuId, condition,
                nodeId, ALLOCATED);
        List<DemandSummary> demandSummaryByDemandType = getDemandSummaryByDemandTypeWithLock(logisticSkuId, condition,
                newArrayList(nodeId), ALLOCATED);
        return demandSummaryByDemandType.stream().mapToDouble(DemandSummary::getCumulativeQuantity).sum();
    }private List<DemandSummary> getDemandSummaryByDemandTypeWithLock(String logisticSkuId, Condition condition, List<String> nodeIds, DemandType demandType) {
        return demandSummaryRepository.getDemandSummaryWithLock(logisticSkuId, condition, nodeIds, demandType);
    }    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "javax.persistence.lock.timeout", value = LOCK_TIMEOUT)})
    List<DemandSummary> getDemandSummaryBySkuConditionNodeIdsAndDemandTypeWithLock(@Param(LOGISTIC_SKU_ID) String logisticSkuId,
                                                                                   @Param(CONDITION) Condition condition,
                                                                                   @Param(NODE_IDS) List<String> nodeIds,
                                                                                   @Param(DEMAND_TYPE) DemandType demandType);
 @Transactional
    public void updateSupplyForSellerDemandAllocation(List<DemandTransaction> demandTransactionList) {
        Set<String> demandSkipNodeIds = getDemandSkipNodeIds();
        List<DemandTransaction> filteredDemandTransactions = demandTransactionList.stream()
                .filter(demandTransaction -> demandSkipNodeIds.contains(demandTransaction.getNodeId())
                        && (demandTransaction.getDemandType() == RESERVED || demandTransaction.getDemandType() == ORDER_CANCELLED))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(filteredDemandTransactions)) {
            return;
        }
        for (DemandTransaction demandTransaction : filteredDemandTransactions) {
            Supply supply = supplyService.getAvailableSupplies(demandTransaction.getNodeId(), demandTransaction.getLogisticSkuId(),
                    Condition.NEW).orElse(null);
            SupplyActivityLog supplyActivityLog;
            if (!Objects.isNull(supply)) {
                AdjustmentType adjustmentType;
                double absQuantity = Math.abs(demandTransaction.getQuantity());
                supply.setUpdatedAt(demandTransaction.getCreatedAt());
                if (demandTransaction.getDemandType() == ORDER_CANCELLED) {
                    supply.incrementQuantityBy(absQuantity);
                    adjustmentType = AdjustmentType.ADD;
                } else if (demandTransaction.getDemandType() == RESERVED) {
                    supply.decrementQuantityBy(absQuantity);
                    adjustmentType = AdjustmentType.SUBTRACT;
                } else {
                    continue;
                }
                supplyActivityLog = supplyActivityBuilder(supply, adjustmentType, UpdateMode.DEMAND_SKIP_SUPPLY_ADJUSTMENT, absQuantity);
                supplyService.updateSupply(supply);
                supplyService.updateSupplyActivityLogs(supplyActivityLog);
            } else {
                log.error("Error in updating supply for seller demand allocation, supply not found for logisticSkuId: {}, " +
                                "nodeId: {} and condition:{} :: Not updating in inventory",
                        demandTransaction.getLogisticSkuId(), demandTransaction.getNodeId(), Condition.NEW);
            }
        }
    }
  public Supply updateSupply(Supply supply) {
        return supplyRepository.save(supply);
    }@Override
    public Supply save(Supply supply) {
        return jpaSupplyRepositoryProxy.save(supply);
    }
