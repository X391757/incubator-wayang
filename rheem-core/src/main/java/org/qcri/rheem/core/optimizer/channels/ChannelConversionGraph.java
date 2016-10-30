package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.util.Bitmask;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This graph contains a set of {@link ChannelConversion}s.
 */
public class ChannelConversionGraph {

    /**
     * Keeps track of the {@link ChannelConversion}s.
     */
    private final Map<ChannelDescriptor, List<ChannelConversion>> conversions = new HashMap<>();

    /**
     * Caches the {@link Comparator} for {@link ProbabilisticDoubleInterval}s.
     */
    private final Comparator<ProbabilisticDoubleInterval> costEstimateComparator;

    private static final Logger logger = LoggerFactory.getLogger(ChannelConversionGraph.class);

    /**
     * Creates a new instance.
     *
     * @param configuration describes how to configure the new instance
     */
    public ChannelConversionGraph(Configuration configuration) {
        this.costEstimateComparator = configuration.getCostEstimateComparatorProvider().provide();
        configuration.getChannelConversionProvider().provideAll().forEach(this::add);
    }

    /**
     * Register a new {@code channelConversion} in this instance, which effectively adds an edge.
     */
    public void add(ChannelConversion channelConversion) {
        final List<ChannelConversion> edges = this.getOrCreateChannelConversions(channelConversion.getSourceChannelDescriptor());
        edges.add(channelConversion);
    }

    /**
     * Return all registered {@link ChannelConversion}s that convert the given {@code channelDescriptor}.
     *
     * @param channelDescriptor should be converted
     * @return the {@link ChannelConversion}s
     */
    private List<ChannelConversion> getOrCreateChannelConversions(ChannelDescriptor channelDescriptor) {
        return this.conversions.computeIfAbsent(channelDescriptor, key -> new ArrayList<>());
    }

    /**
     * Finds the minimum tree {@link Junction} (w.r.t. {@link TimeEstimate}s that connects the given {@link OutputSlot} to the
     * {@code destInputSlots}.
     *
     * @param output              {@link OutputSlot} of an {@link ExecutionOperator} that should be consumed
     * @param destInputSlots      {@link InputSlot}s of {@link ExecutionOperator}s that should receive data from the {@code output}
     * @param optimizationContext describes the above mentioned {@link ExecutionOperator} key figures
     * @return a {@link Junction} or {@code null} if none could be found
     */
    public Junction findMinimumCostJunction(OutputSlot<?> output,
                                            List<InputSlot<?>> destInputSlots,
                                            OptimizationContext optimizationContext) {
        return this.findMinimumCostJunction(output, null, destInputSlots, optimizationContext);
    }

    /**
     * Finds the minimum tree {@link Junction} (w.r.t. {@link TimeEstimate}s that connects the given {@link OutputSlot} to the
     * {@code destInputSlots}.
     *
     * @param output              {@link OutputSlot} of an {@link ExecutionOperator} that should be consumed
     * @param openChannels        existing {@link Channel}s that must be part of the tree or {@code null}
     * @param destInputSlots      {@link InputSlot}s of {@link ExecutionOperator}s that should receive data from the {@code output}
     * @param optimizationContext describes the above mentioned {@link ExecutionOperator} key figures
     * @return a {@link Junction} or {@code null} if none could be found
     */
    public Junction findMinimumCostJunction(OutputSlot<?> output,
                                            Collection<Channel> openChannels,
                                            List<InputSlot<?>> destInputSlots,
                                            OptimizationContext optimizationContext) {
        return new ShortestTreeSearcher(output, openChannels, destInputSlots, optimizationContext).getJunction();
    }

    /**
     * Given two {@link Tree}s, choose the one with lower costs.
     */
    private Tree selectCheaperTree(Tree t1, Tree t2) {
        return this.costEstimateComparator.compare(t1.costs, t2.costs) <= 0 ? t1 : t2;
    }

    /**
     * Merge several {@link Tree}s, which should have the same root but should be otherwise disjoint.
     *
     * @return the merged {@link Tree} or {@code null} if the input {@code trees} could not be merged
     */
    private Tree mergeTrees(Collection<Tree> trees) {
        assert trees.size() >= 2;

        // For various trees to be combined, we require them to be "disjoint". Check this.
        // TODO: This might be a little bit too strict.
        final Iterator<Tree> iterator = trees.iterator();
        final Tree firstTree = iterator.next();
        Bitmask combinationSettledIndices = new Bitmask(firstTree.settledDestinationIndices);
        int maxSettledIndices = combinationSettledIndices.cardinality();
        final HashSet<ChannelDescriptor> employedChannelDescriptors = new HashSet<>(firstTree.employedChannelDescriptors);
        int maxVisitedChannelDescriptors = employedChannelDescriptors.size();
        ProbabilisticDoubleInterval costs = firstTree.costs;
        TreeVertex newRoot = new TreeVertex(firstTree.root.channelDescriptor, firstTree.root.settledIndices);
        newRoot.copyEdgesFrom(firstTree.root);

        while (iterator.hasNext()) {
            final Tree ithTree = iterator.next();

            combinationSettledIndices.orInPlace(ithTree.settledDestinationIndices);
            maxSettledIndices += ithTree.settledDestinationIndices.cardinality();
            if (maxSettledIndices > combinationSettledIndices.cardinality()) {
                return null;
            }
            employedChannelDescriptors.addAll(ithTree.employedChannelDescriptors);
            maxVisitedChannelDescriptors += ithTree.employedChannelDescriptors.size() - 1; // NB: -1 for the root
            if (maxVisitedChannelDescriptors > employedChannelDescriptors.size()) {
                return null;
            }

            costs = costs.plus(ithTree.costs);
            newRoot.copyEdgesFrom(ithTree.root);
        }

        // If all tests are passed, create the combination.
        final Tree mergedTree = new Tree(newRoot, combinationSettledIndices);
        mergedTree.costs = costs;
        mergedTree.employedChannelDescriptors.addAll(employedChannelDescriptors);
        return mergedTree;
    }

    /**
     * Finds the shortest tree between the {@link #sourceChannelDescriptor} and the {@link #destChannelDescriptorSets}.
     */
    private class ShortestTreeSearcher extends OneTimeExecutable {

        /**
         * The {@link OutputSlot} that should be converted.
         */
        private final OutputSlot<?> sourceOutput;

        /**
         * {@link ChannelDescriptor} for the {@link Channel} produced by the {@link #sourceOutput}.
         */
        private final ChannelDescriptor sourceChannelDescriptor;

        /**
         * Describes the number of data quanta that are presumably converted.
         */
        private final CardinalityEstimate cardinality;

        /**
         * How often the conversion is presumably performed.
         */
        private final int numExecutions;

        /**
         * {@link ChannelDescriptor}s of {@link #existingChannels} that are allowed to have new consumers.
         */
        private final Set<ChannelDescriptor> openChannelDescriptors;

        /**
         * Maps {@link ChannelDescriptor}s to already existing {@link Channel}s.
         */
        private final Map<ChannelDescriptor, Channel> existingChannels;

        /**
         * Maps destination {@link InputSlot}s to {@link #existingChannels} that are their destination.
         */
        private final Map<InputSlot<?>, Channel> existingDestinationChannels;

        private final Bitmask existingDestinationChannelIndices,
                absentDestinationChannelIndices,
                allDestinationChannelIndices = new Bitmask();

        private final Map<ChannelDescriptor, Bitmask> reachableExistingDestinationChannelIndices;

        /**
         * {@link InputSlot}s that should be served by the {@link #sourceOutput}.
         */
        private final List<InputSlot<?>> destInputs;

        /**
         * Supported {@link ChannelDescriptor}s for each of the {@link #destInputs}.
         */
        private final List<Set<ChannelDescriptor>> destChannelDescriptorSets;

        /**
         * The input {@link OptimizationContext} (and a write-copy, repectively).
         */
        private final OptimizationContext optimizationContext, optimizationContextCopy;

        /**
         * Maps kernelized {@link Set}s of possible input {@link ChannelDescriptor}s to destination {@link InputSlot}s via
         * their respective indices in {@link #destInputs}.
         */
        private Map<Set<ChannelDescriptor>, Bitmask> kernelDestChannelDescriptorSetsToIndices;

        /**
         * Maps specific input {@link ChannelDescriptor}s to applicable destination {@link InputSlot}s via
         * their respective indices in {@link #destInputs}.
         */
        private Map<ChannelDescriptor, Bitmask> kernelDestChannelDescriptorsToIndices;

        /**
         * Caches cost estimates for {@link ChannelConversion}s.
         */
        private Map<ChannelConversion, ProbabilisticDoubleInterval> conversionCostCache = new HashMap<>();

        /**
         * Caches the result of {@link #getJunction()}.
         */
        private Junction result = null;

        /**
         * Create a new instance.
         *
         * @param sourceOutput        provides a {@link Channel} that should be converted
         * @param openChannels        existing {@link Channel} derived from {@code sourceOutput} and where we must take up
         *                            the search; or {@code null}
         * @param destInputs          that consume the converted {@link Channel}(s)
         * @param optimizationContext provides optimization info
         */
        private ShortestTreeSearcher(OutputSlot<?> sourceOutput,
                                     Collection<Channel> openChannels,
                                     List<InputSlot<?>> destInputs,
                                     OptimizationContext optimizationContext) {

            // Store relevant variables.
            this.optimizationContext = optimizationContext;
            this.optimizationContextCopy = new DefaultOptimizationContext(this.optimizationContext);
            this.sourceOutput = sourceOutput;
            this.destInputs = destInputs;
            final boolean isOpenChannelsPresent = openChannels != null && !openChannels.isEmpty();

            // Figure out the optimization info via the sourceOutput.
            final ExecutionOperator outputOperator = (ExecutionOperator) this.sourceOutput.getOwner();
            final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(outputOperator);
            assert operatorContext != null : String.format("Optimization info for %s missing.", outputOperator);
            this.cardinality = operatorContext.getOutputCardinality(this.sourceOutput.getIndex());
            this.numExecutions = operatorContext.getNumExecutions();

            // Figure out, if a part of the conversion is already in place and initialize accordingly.

            if (isOpenChannelsPresent) {
                // Take any open channel and trace it back to the source channel.
                Channel existingChannel = RheemCollections.getAny(openChannels);
                while (existingChannel.getProducerSlot() != sourceOutput) {
                    existingChannel = OptimizationUtils.getPredecessorChannel(existingChannel);
                }
                final Channel sourceChannel = existingChannel;
                this.sourceChannelDescriptor = sourceChannel.getDescriptor();

                // Now traverse down-stream to find already reached destinations.
                this.existingChannels = new HashMap<>();
                this.existingDestinationChannels = new HashMap<>(4);
                this.existingDestinationChannelIndices = new Bitmask();

                this.collectExistingChannels(sourceChannel);
                this.openChannelDescriptors = new HashSet<>(openChannels.size());
                for (Channel openChannel : openChannels) {
                    this.openChannelDescriptors.add(openChannel.getDescriptor());
                }
            } else {
                this.sourceChannelDescriptor = outputOperator.getOutputChannelDescriptor(this.sourceOutput.getIndex());
                this.existingChannels = Collections.emptyMap();
                this.existingDestinationChannels = Collections.emptyMap();
                this.existingDestinationChannelIndices = new Bitmask();
                this.openChannelDescriptors = Collections.emptySet();
            }


            // Set up the destinations.
            this.destChannelDescriptorSets = RheemCollections.map(destInputs, this::resolveSupportedChannels);
            assert this.destChannelDescriptorSets.stream().noneMatch(Collection::isEmpty);
            this.kernelizeChannelRequests();

            if (isOpenChannelsPresent) {
                // Update the bitmask of all already reached destination channels...
                // ...and mark the paths of already reached destination channels via upstream traversal.
                this.reachableExistingDestinationChannelIndices = new HashMap<>();
                for (Channel existingDestinationChannel : this.existingDestinationChannels.values()) {
                    final Bitmask channelIndices = this.kernelDestChannelDescriptorsToIndices
                            .get(existingDestinationChannel.getDescriptor())
                            .and(this.existingDestinationChannelIndices);
                    while (true) {
                        this.reachableExistingDestinationChannelIndices.compute(
                                existingDestinationChannel.getDescriptor(),
                                (k, v) -> v == null ? new Bitmask(channelIndices) : v.orInPlace(channelIndices)
                        );
                        if (existingDestinationChannel.getDescriptor().equals(this.sourceChannelDescriptor)) break;
                        existingDestinationChannel = OptimizationUtils.getPredecessorChannel(existingDestinationChannel);
                    }
                }
            } else {
                this.reachableExistingDestinationChannelIndices = Collections.emptyMap();
            }

            this.absentDestinationChannelIndices = this.allDestinationChannelIndices
                    .andNot(this.existingDestinationChannelIndices);
        }

        /**
         * Traverse the given {@link Channel} downstream and collect any {@link Channel} that feeds some
         * {@link InputSlot} of a non-conversion {@link ExecutionOperator}.
         *
         * @param channel the {@link Channel} to traverse from
         * @see #existingChannels
         * @see #existingDestinationChannels
         */
        private void collectExistingChannels(Channel channel) {
            this.existingChannels.put(channel.getDescriptor(), channel);
            for (ExecutionTask consumer : channel.getConsumers()) {
                final ExecutionOperator operator = consumer.getOperator();
                if (!operator.isAuxiliary()) {
                    final InputSlot<?> input = consumer.getInputSlotFor(channel);
                    this.existingDestinationChannels.put(input, channel);
                    int destIndex = 0;
                    while (this.destInputs.get(destIndex) != input) destIndex++;
                    this.existingDestinationChannelIndices.set(destIndex);
                } else {
                    for (Channel outputChannel : consumer.getOutputChannels()) {
                        if (outputChannel != null) this.collectExistingChannels(outputChannel);
                    }
                }
            }
        }

        /**
         * Creates and caches a {@link Junction} according to the initialization parameters.
         *
         * @return the {@link Junction} or {@code null} if none could be found
         */
        public Junction getJunction() {
            this.tryExecute();
            return this.result;
        }

        /**
         * Find the supported {@link ChannelDescriptor}s for the given {@link InputSlot}. If the latter is a
         * "loop invariant" {@link InputSlot}, then require to only reusable {@link ChannelDescriptor}.
         *
         * @param input for which supported {@link ChannelDescriptor}s are requested
         * @return all eligible {@link ChannelDescriptor}s
         */
        private Set<ChannelDescriptor> resolveSupportedChannels(final InputSlot<?> input) {
            final Channel existingChannel = this.existingDestinationChannels.get(input);
            if (existingChannel != null) {
                return Collections.singleton(existingChannel.getDescriptor());
            }

            final ExecutionOperator owner = (ExecutionOperator) input.getOwner();
            final List<ChannelDescriptor> supportedInputChannels = owner.getSupportedInputChannels(input.getIndex());
            if (input.isLoopInvariant()) {
                // Loop input is needed in several iterations and must therefore be reusable.
                return supportedInputChannels.stream().filter(ChannelDescriptor::isReusable).collect(Collectors.toSet());
            } else {
                return RheemCollections.asSet(supportedInputChannels);
            }
        }

        @Override
        protected void doExecute() {
            // Start from the root vertex.
            final Tree tree = this.searchTree();
            if (tree != null) {
                this.createJunction(tree);
            } else {
                logger.debug("Could not connect {} with {}.", this.sourceOutput, this.destInputs);
            }
        }

        /**
         * Rule out any non-reusable {@link ChannelDescriptor}s in recurring {@link ChannelDescriptor} sets.
         *
         * @see #kernelDestChannelDescriptorSetsToIndices
         * @see #kernelDestChannelDescriptorsToIndices
         */
        private void kernelizeChannelRequests() {
            // Check if the Junction enters a loop "from the side", i.e., across multiple iterations.
            // CHECK: Since we rule out non-reusable Channels in #resolveSupportedChannels, do we really need this?
//            final LoopSubplan outputLoop = this.sourceOutput.getOwner().getInnermostLoop();
//            final int outputLoopDepth = this.sourceOutput.getOwner().getLoopStack().size();
//            boolean isSideEnterLoop = this.destInputs.stream().anyMatch(input ->
//                    !input.getOwner().isLoopHead() &&
//                            (input.getOwner().getLoopStack().size() > outputLoopDepth ||
//                                    (input.getOwner().getLoopStack().size() == outputLoopDepth && input.getOwner().getInnermostLoop() != outputLoop)
//                            )
//            );


            // Index the (unreached) Channel requests by their InputSlots, thereby merging equal ones.
            int index = 0;
            this.kernelDestChannelDescriptorSetsToIndices = new HashMap<>(this.destChannelDescriptorSets.size());
            for (Set<ChannelDescriptor> destChannelDescriptorSet : this.destChannelDescriptorSets) {
                final Bitmask indices = this.kernelDestChannelDescriptorSetsToIndices.computeIfAbsent(
                        destChannelDescriptorSet, key -> new Bitmask(this.destChannelDescriptorSets.size())
                );
                this.allDestinationChannelIndices.set(index);
                indices.set(index++);
            }

            // Strip off the non-reusable, superfluous ChannelDescriptors where applicable.
            Collection<Tuple<Set<ChannelDescriptor>, Bitmask>> kernelDestChannelDescriptorSetsToIndicesUpdates = new LinkedList<>();
            final Iterator<Map.Entry<Set<ChannelDescriptor>, Bitmask>> iterator =
                    this.kernelDestChannelDescriptorSetsToIndices.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<Set<ChannelDescriptor>, Bitmask> entry = iterator.next();
                final Bitmask indices = entry.getValue();
                // Don't touch destination channel sets that occur only once.
                if (indices.cardinality() < 2) continue;

                // If there is exactly one non-reusable and more than one reusable channel, we can remove the
                // non-reusable one.
                Set<ChannelDescriptor> channelDescriptors = entry.getKey();
                int numReusableChannels = (int) channelDescriptors.stream().filter(ChannelDescriptor::isReusable).count();
                if (numReusableChannels == 0 && channelDescriptors.size() == 1) {
                    logger.warn(
                            "More than two target operators request only the non-reusable channel {}.",
                            RheemCollections.getSingle(channelDescriptors)
                    );
                }
                if (channelDescriptors.size() - numReusableChannels == 1) {
                    iterator.remove();
                    channelDescriptors = new HashSet<>(channelDescriptors);
                    channelDescriptors.removeIf(channelDescriptor -> !channelDescriptor.isReusable());
                    kernelDestChannelDescriptorSetsToIndicesUpdates.add(new Tuple<>(channelDescriptors, indices));
                }
            }
            for (Tuple<Set<ChannelDescriptor>, Bitmask> channelsToIndicesChange : kernelDestChannelDescriptorSetsToIndicesUpdates) {
                this.kernelDestChannelDescriptorSetsToIndices.computeIfAbsent(
                        channelsToIndicesChange.getField0(),
                        key -> new Bitmask(this.destChannelDescriptorSets.size())
                ).orInPlace(channelsToIndicesChange.getField1());
            }

            // Index the single ChannelDescriptors.
            this.kernelDestChannelDescriptorsToIndices = new HashMap<>();
            for (Map.Entry<Set<ChannelDescriptor>, Bitmask> entry : this.kernelDestChannelDescriptorSetsToIndices.entrySet()) {
                final Set<ChannelDescriptor> channelDescriptorSet = entry.getKey();
                final Bitmask indices = entry.getValue();

                for (ChannelDescriptor channelDescriptor : channelDescriptorSet) {
                    this.kernelDestChannelDescriptorsToIndices.merge(channelDescriptor, new Bitmask(indices), Bitmask::or);
                }
            }
        }

        /**
         * Starts the actual search.
         */
        private Tree searchTree() {
            // Prepare the recursive traversal.
            final HashSet<ChannelDescriptor> visitedChannelDescriptors = new HashSet<>(16);

            // Perform the traversal.
            final Map<Bitmask, Tree> solutions = this.enumerate(
                    visitedChannelDescriptors,
                    this.sourceChannelDescriptor,
                    Bitmask.EMPTY_BITMASK
            );

            // Get hold of a comprehensive solution (if it exists).
            Bitmask requestedIndices = new Bitmask(this.destChannelDescriptorSets.size());
            requestedIndices.flip(0, this.destChannelDescriptorSets.size());
            return solutions.get(requestedIndices);
        }

        /**
         * Recursive {@link Tree} enumeration strategy.
         *
         * @param visitedChannelDescriptors previously visited {@link ChannelDescriptor}s (inclusive of {@code channelDescriptor};
         *                                  can be altered but must be in original state before leaving the method
         * @param channelDescriptor         the currently enumerated {@link ChannelDescriptor}
         * @param settledDestinationIndices indices of destinations that have already been reached via the
         *                                  {@code visitedChannelDescriptors} (w/o {@code channelDescriptor};
         *                                  can be altered but must be in original state before leaving the method
         * @return solutions to the search problem reachable from this node; {@link Tree}s must still be rerooted
         */
        public Map<Bitmask, Tree> enumerate(
                Set<ChannelDescriptor> visitedChannelDescriptors,
                ChannelDescriptor channelDescriptor,
                Bitmask settledDestinationIndices) {

            // Mapping from settled indices to the cheapest tree settling them. Will be the return value.
            Map<Bitmask, Tree> newSolutions = new HashMap<>(16);
            Tree newSolution;


            // Check if current path is a (new) solution.
            // Exclude existing destinations that are not reached via the current path.
            final Bitmask excludedExistingIndices = this.existingDestinationChannelIndices.andNot(
                    this.reachableExistingDestinationChannelIndices.getOrDefault(channelDescriptor, Bitmask.EMPTY_BITMASK)
            );
            // Exclude missing destinations if the current channel exists but is not open.
            final Bitmask excludedAbsentIndices =
                    (this.existingChannels.containsKey(channelDescriptor) && !openChannelDescriptors.contains(channelDescriptor)) ?
                            this.absentDestinationChannelIndices :
                            Bitmask.EMPTY_BITMASK;
            final Bitmask newSettledIndices = this.kernelDestChannelDescriptorsToIndices
                    .getOrDefault(channelDescriptor, Bitmask.EMPTY_BITMASK)
                    .andNot(settledDestinationIndices)
                    .andNotInPlace(excludedExistingIndices)
                    .andNotInPlace(excludedAbsentIndices);

            if (!newSettledIndices.isEmpty()) {
                if (channelDescriptor.isReusable() || newSettledIndices.cardinality() == 1) {
                    // If the channel is reusable, it is almost safe to say that we either use it for all possible
                    // destinations or for none, because no extra costs can incur.
                    // TODO: Create all possible combinations if required.
                    // The same is for when there is only a single destination reached.
                    newSolution = Tree.singleton(channelDescriptor, newSettledIndices);
                    newSolutions.put(newSolution.settledDestinationIndices, newSolution);

                } else {
                    // Otherwise, create an entry for each settled index.
                    for (int index = newSettledIndices.nextSetBit(0); index != -1; index = newSettledIndices.nextSetBit(index + 1)) {
                        Bitmask newSettledIndicesSubset = new Bitmask(index + 1);
                        newSettledIndicesSubset.set(index);
                        newSolution = Tree.singleton(channelDescriptor, newSettledIndicesSubset);
                        newSolutions.put(newSolution.settledDestinationIndices, newSolution);
                    }
                }

                // Check early stopping criteria:
                // There are no more channels that could be settled.
                if (newSettledIndices.cardinality() == this.destChannelDescriptorSets.size() - excludedExistingIndices.cardinality()) {
                    return newSolutions;
                }
            }

            // For each outgoing edge, explore all combinations of reachable target indices.
            if (channelDescriptor.isReusable()) {
                // When descending, "pick" the newly settled destinations only for reusable ChannelDescriptors.
                settledDestinationIndices.orInPlace(newSettledIndices);
            }
            final List<ChannelConversion> channelConversions =
                    ChannelConversionGraph.this.conversions.getOrDefault(channelDescriptor, Collections.emptyList());
            final List<Collection<Tree>> childSolutionSets = new ArrayList<>(channelConversions.size());
            final Set<ChannelDescriptor> successorChannelDescriptors = this.getSuccessorChannelDescriptors(channelDescriptor);
            for (ChannelConversion channelConversion : channelConversions) {
                final ChannelDescriptor targetChannelDescriptor = channelConversion.getTargetChannelDescriptor();

                // Skip if there are successor channel descriptors that do not contain the target channel descriptor.
                if (successorChannelDescriptors != null && !successorChannelDescriptors.contains(targetChannelDescriptor)) {
                    continue;
                }

                if (visitedChannelDescriptors.add(targetChannelDescriptor)) {
                    final Map<Bitmask, Tree> childSolutions = this.enumerate(
                            visitedChannelDescriptors,
                            targetChannelDescriptor,
                            settledDestinationIndices
                    );
                    childSolutions.values().forEach(
                            tree -> tree.reroot(
                                    channelDescriptor,
                                    channelDescriptor.isReusable() ? newSettledIndices : Bitmask.EMPTY_BITMASK,
                                    channelConversion,
                                    this.getCostEstimate(channelConversion)
                            )
                    );
                    childSolutionSets.add(childSolutions.values());

                    visitedChannelDescriptors.remove(targetChannelDescriptor);
                }
            }
            settledDestinationIndices.andNotInPlace(newSettledIndices);

            // Merge the childSolutionSets into the newSolutions.
            // Each childSolutionSet corresponds to a traversed outgoing ChannelConversion.

            // At first, consider the childSolutionSet for each outgoing ChannelConversion individually.
            for (Collection<Tree> childSolutionSet : childSolutionSets) {
                // Each childSolutionSet its has a mapping from settled indices to trees.
                for (Tree tree : childSolutionSet) {
                    // Update newSolutions if the current tree is cheaper or settling new indices.
                    newSolutions.merge(tree.settledDestinationIndices, tree, ChannelConversionGraph.this::selectCheaperTree);
                }
            }


            // If the current Channel/vertex is reusable, also detect valid combinations.
            // Check if the combinations yield new solutions.
            if (channelDescriptor.isReusable()
                    && this.kernelDestChannelDescriptorSetsToIndices.size() > 1
                    && childSolutionSets.size() > 1
                    && this.destInputs.size() > newSettledIndices.cardinality() + settledDestinationIndices.cardinality() + 1) {

                // Determine the number of "unreached" destChannelDescriptorSets.
                int numUnreachedDestinationSets = 0;
                for (Bitmask settlableDestinationIndices : this.kernelDestChannelDescriptorSetsToIndices.values()) {
                    if (!settlableDestinationIndices.isSubmaskOf(settledDestinationIndices)) {
                        numUnreachedDestinationSets++;
                    }
                }

                if (numUnreachedDestinationSets >= 2) {
                    final Collection<List<Collection<Tree>>> childSolutionSetCombinations =
                            RheemCollections.createPowerList(childSolutionSets, numUnreachedDestinationSets);
                    childSolutionSetCombinations.removeIf(e -> e.size() < 2);
                    for (List<Tree> solutionCombination : RheemCollections.streamedCrossProduct(childSolutionSets)) {
                        final Tree tree = ChannelConversionGraph.this.mergeTrees(solutionCombination);
                        if (tree != null) {
                            newSolutions.merge(tree.settledDestinationIndices, tree, ChannelConversionGraph.this::selectCheaperTree);
                        }
                    }
                }
            }

            return newSolutions;
        }

        /**
         * Find {@link ChannelDescriptor}s of {@link Channel}s that can be reached from the current
         * {@link ChannelDescriptor}. This is relevant only when there are {@link #existingChannels}.
         *
         * @param descriptor from which successor {@link ChannelDescriptor}s are requested
         * @return the successor {@link ChannelDescriptor}s or {@code null} if no restrictions apply
         */
        private Set<ChannelDescriptor> getSuccessorChannelDescriptors(ChannelDescriptor descriptor) {
            final Channel channel = this.existingChannels.get(descriptor);
            if (channel == null || this.openChannelDescriptors.contains(descriptor)) return null;

            Set<ChannelDescriptor> result = new HashSet<>();
            for (ExecutionTask consumer : channel.getConsumers()) {
                if (!consumer.getOperator().isAuxiliary()) continue;
                for (Channel successorChannel : consumer.getOutputChannels()) {
                    result.add(successorChannel.getDescriptor());
                }
            }

            return result;
        }

        /**
         * Retrieve a cached or calculate and cache the cost estimate for a given {@link ChannelConversion}
         * w.r.t. the {@link #cardinality}.
         *
         * @param channelConversion whose cost estimate is requested
         * @return the cost estimate
         */
        private ProbabilisticDoubleInterval getCostEstimate(ChannelConversion channelConversion) {
            return this.conversionCostCache.computeIfAbsent(
                    channelConversion,
                    key -> key.estimateConversionCost(this.cardinality, this.numExecutions, this.optimizationContextCopy)
            );
        }

        private void createJunction(Tree tree) {
            Collection<OptimizationContext> localOptimizationContexts = this.forkLocalOptimizationContext();

            // Create the a new Junction.
            final Junction junction = new Junction(this.sourceOutput, this.destInputs, localOptimizationContexts);
            Channel sourceChannel = this.existingChannels.get(this.sourceChannelDescriptor);
            if (sourceChannel == null) {
                sourceChannel = this.sourceChannelDescriptor.createChannel(this.sourceOutput, this.optimizationContext.getConfiguration());
            }
            junction.setSourceChannel(sourceChannel);
            this.createJunctionAux(tree.root, sourceChannel, junction);

            // Assign appropriate LoopSubplans to the newly created ExecutionTasks.
            // Determine the LoopSubplan from the "source side" of the Junction.
            final OutputSlot<?> sourceOutput = sourceChannel.getProducerSlot();
            final ExecutionOperator sourceOperator = (ExecutionOperator) sourceOutput.getOwner();
            final LoopSubplan sourceLoop =
                    (!sourceOperator.isLoopHead() || sourceOperator.isFeedforwardOutput(sourceOutput)) ?
                            sourceOperator.getInnermostLoop() : null;

            if (sourceLoop != null) {
                // If the source side is determining a LoopSubplan, it should be what the "target sides" request.
                for (int destIndex = 0; destIndex < this.destInputs.size(); destIndex++) {
                    assert this.destInputs.get(destIndex).getOwner().getInnermostLoop() == sourceLoop :
                            String.format(
                                    "Expected that %s would belong to %s, just as %s does.",
                                    this.destInputs.get(destIndex), sourceLoop, sourceOutput
                            );
                    Channel targetChannel = junction.getTargetChannel(destIndex);
                    while (targetChannel != sourceChannel) {
                        final ExecutionTask producer = targetChannel.getProducer();
                        producer.getOperator().setContainer(sourceLoop);
                        assert producer.getNumInputChannels() == 1 : String.format(
                                "Glue operator %s was expected to have exactly one input channel.",
                                producer
                        );
                        targetChannel = producer.getInputChannel(0);
                    }
                }
            }

            // CHECK: We don't need to worry about entering loops, because in this case #resolveSupportedChannels(...)
            // does all the magic!?

            this.result = junction;
        }

        /**
         * Helper function to create a {@link Junction} from a {@link Tree}.
         *
         * @param vertex      the currently iterated {@link TreeVertex} (start at the root)
         * @param baseChannel the corresponding {@link Channel}
         * @param junction    that is being initialized
         */
        private void createJunctionAux(TreeVertex vertex, Channel baseChannel, Junction junction) {
            Channel baseChannelCopy = null;
            for (int index = vertex.settledIndices.nextSetBit(0);
                 index >= 0;
                 index = vertex.settledIndices.nextSetBit(index + 1)) {
                // Beware that, if the base channel is existent (and open), we need to create a copy for any new
                // destinations.
                if (!this.existingDestinationChannelIndices.get(index) && this.openChannelDescriptors.contains(baseChannel.getDescriptor())) {
                    if (baseChannelCopy == null) baseChannelCopy = baseChannel.copy();
                    junction.setTargetChannel(index, baseChannelCopy);
                } else {
                    junction.setTargetChannel(index, baseChannel);
                }
            }

            for (TreeEdge edge : vertex.outEdges) {
                // See if there is an already existing channel in place.
                Channel newChannel = this.existingChannels.get(edge.channelConversion.getTargetChannelDescriptor());

                // Otherwise, create a new channel conversion.
                if (newChannel == null) {
                    // Beware that, if the base channel is existent (and open), we need to create a copy of it for the
                    // new channel conversions.
                    if (baseChannelCopy == null) {
                        baseChannelCopy = this.openChannelDescriptors.contains(baseChannel.getDescriptor()) ?
                                baseChannel.copy() :
                                baseChannel;
                    }
                    newChannel = edge.channelConversion.convert(
                            baseChannelCopy,
                            this.optimizationContext.getConfiguration(),
                            junction.getOptimizationContexts(),
                            // Hacky: Inject cardinality for cases where we convert a LoopHeadOperator output.
                            junction.getOptimizationContexts().size() == 1 ? this.cardinality : null
                    );
                }
                if (baseChannel != newChannel) {
                    final ExecutionTask producer = newChannel.getProducer();
                    final ExecutionOperator conversionOperator = producer.getOperator();
                    conversionOperator.setName(String.format(
                            "convert %s", junction.getSourceOutput()
                    ));
                    junction.register(producer);
                }

                this.createJunctionAux(edge.destination, newChannel, junction);
            }
        }

        /**
         * Creates a new {@link OptimizationContext} that forks
         * <ul>
         * <li>the given {@code optimizationContext}'s parent if the {@link #sourceOutput} is the final
         * {@link OutputSlot} of a {@link LoopHeadOperator}</li>
         * <li>or else the given {@code optimizationContext}.</li>
         * </ul>
         * We have to do this because in the former case the {@link Junction} {@link ExecutionOperator}s should not
         * reside in a loop {@link OptimizationContext}.
         *
         * @return the forked {@link OptimizationContext}
         */
        // TODO: Refactor this.
        private Collection<OptimizationContext> forkLocalOptimizationContext() {
            OptimizationContext baseOptimizationContext =
                    this.sourceOutput.getOwner().isLoopHead() && !this.sourceOutput.isFeedforward() ?
                            this.optimizationContext.getParent() :
                            this.optimizationContext;
            return baseOptimizationContext.getDefaultOptimizationContexts().stream()
                    .map(DefaultOptimizationContext::new)
                    .collect(Collectors.toList());
        }


    }

    /**
     * A tree consisting of {@link TreeVertex}es connected by {@link TreeEdge}s.
     */
    private static class Tree {

        /**
         * The root node of this instance.
         */
        private TreeVertex root;

        /**
         * The union of all settled indices of the contained {@link TreeVertex}es in this instance.
         *
         * @see TreeVertex#settledIndices
         */
        private final Bitmask settledDestinationIndices;

        /**
         * The {@link Set} of {@link ChannelDescriptor}s in all {@link TreeVertex}es of this instance.
         *
         * @see TreeVertex#channelDescriptor
         */
        private final Set<ChannelDescriptor> employedChannelDescriptors = new HashSet<>();

        /**
         * The sum of {@link TimeEstimate}s of all {@link TreeEdge}s of this instance.
         */
        private ProbabilisticDoubleInterval costs = ProbabilisticDoubleInterval.zero;

        /**
         * Creates a new instance with a single {@link TreeVertex}.
         *
         * @param channelDescriptor represented by the {@link TreeVertex}
         * @param settledIndices    indices to destinations settled by the {@code channelDescriptor}
         * @return the new instance
         */
        static Tree singleton(ChannelDescriptor channelDescriptor, Bitmask settledIndices) {
            return new Tree(new TreeVertex(channelDescriptor, settledIndices), new Bitmask(settledIndices));
        }

        Tree(TreeVertex root, Bitmask settledDestinationIndices) {
            this.root = root;
            this.settledDestinationIndices = settledDestinationIndices;
            this.employedChannelDescriptors.add(root.channelDescriptor);
        }

        /**
         * Push down the {@link #root} of this instance by adding a new {@link TreeVertex} as root and put the old
         * root as its child node.
         *
         * @param newRootChannelDescriptor    will be wrapped in the new {@link #root}
         * @param newRootSettledIndices       destination indices settled by the {@code newRootChannelDescriptor}
         * @param newToObsoleteRootConversion used to establish the {@link TreeEdge} between the old and new {@link #root}
         * @param costEstimate                of the {@code newToObsoleteRootConversion}
         */
        void reroot(ChannelDescriptor newRootChannelDescriptor,
                    Bitmask newRootSettledIndices,
                    ChannelConversion newToObsoleteRootConversion,
                    ProbabilisticDoubleInterval costEstimate) {
            // Exchange the root.
            final TreeVertex newRoot = new TreeVertex(newRootChannelDescriptor, newRootSettledIndices);
            final TreeEdge edge = newRoot.linkTo(newToObsoleteRootConversion, this.root, costEstimate);
            this.root = newRoot;
            // Update metadata.
            this.employedChannelDescriptors.add(newRootChannelDescriptor);
            this.settledDestinationIndices.orInPlace(newRootSettledIndices);
            this.costs = this.costs.plus(edge.costEstimate);
        }

        @Override
        public String toString() {
            return String.format("%s[%s, %s]", this.getClass().getSimpleName(), this.costs, this.root.getChildChannelConversions());
        }
    }

    /**
     * Vertex in a {@link Tree}. Corresponds to a {@link ChannelDescriptor}.
     */
    private static class TreeVertex {

        /**
         * The {@link ChannelDescriptor} represented by this instance.
         */
        private final ChannelDescriptor channelDescriptor;

        /**
         * {@link TreeEdge}s to child {@link TreeVertex}es.
         */
        private final List<TreeEdge> outEdges;

        /**
         * Indices to settled {@link ShortestTreeSearcher#destInputs}.
         */
        private final Bitmask settledIndices;

        /**
         * Creates a new instance.
         *
         * @param channelDescriptor to be represented by this instance
         * @param settledIndices    indices to settled destinations
         */
        private TreeVertex(ChannelDescriptor channelDescriptor, Bitmask settledIndices) {
            this.channelDescriptor = channelDescriptor;
            this.settledIndices = settledIndices;
            this.outEdges = new ArrayList<>(4);
        }

        private TreeEdge linkTo(ChannelConversion channelConversion, TreeVertex destination, ProbabilisticDoubleInterval costEstimate) {
            final TreeEdge edge = new TreeEdge(channelConversion, destination, costEstimate);
            this.outEdges.add(edge);
            return edge;
        }

        private void copyEdgesFrom(TreeVertex that) {
            assert this.channelDescriptor.equals(that.channelDescriptor);
            this.outEdges.addAll(that.outEdges);
        }

        /**
         * Collects all {@link ChannelConversion}s employed by (indirectly) outgoing {@link TreeEdge}s.
         *
         * @return a {@link Set} of said {@link ChannelConversion}s
         */
        private Set<ChannelConversion> getChildChannelConversions() {
            Set<ChannelConversion> channelConversions = new HashSet<>();
            for (TreeEdge edge : this.outEdges) {
                channelConversions.add(edge.channelConversion);
                channelConversions.addAll(edge.destination.getChildChannelConversions());
            }
            return channelConversions;
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", this.getClass().getSimpleName(), this.channelDescriptor);
        }
    }

    /**
     * Edge in a {@link Tree}.
     */
    private static class TreeEdge {

        /**
         * The target {@link TreeVertex}.
         */
        private final TreeVertex destination;

        /**
         * The {@link ChannelConversion} represented by this instance.
         */
        private final ChannelConversion channelConversion;

        /**
         * The cost estimate of the {@link #channelConversion}.
         */
        private final ProbabilisticDoubleInterval costEstimate;

        private TreeEdge(ChannelConversion channelConversion, TreeVertex destination, ProbabilisticDoubleInterval costEstimate) {
            this.channelConversion = channelConversion;
            this.destination = destination;
            this.costEstimate = costEstimate;
        }

        @Override
        public String toString() {
            return String.format("%s[%s, %s]", this.getClass().getSimpleName(), this.channelConversion, this.costEstimate);
        }
    }
}
