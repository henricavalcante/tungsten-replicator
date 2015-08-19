/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.cluster.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.continuent.tungsten.common.exception.ClusterMembershipValidationException;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Encapsulates the logic to determine whether a set of members represents a
 * primary partition that may continue to operate as a cluster. This class also
 * includes logic to determine whether the current GC view is consistent and
 * whether the digest itself is valid.
 */
public class ClusterMembershipDigest
{
    // Base data.
    private String                         name;
    private Vector<String>                 configuredDBMembers       = new Vector<String>();
    private Vector<String>                 configuredActiveWitnesses = new Vector<String>();
    private Vector<String>                 viewDBMembers             = new Vector<String>();
    private Vector<String>                 viewActiveWitnessMembers  = new Vector<String>();
    private Vector<String>                 consolidatedViewMembers   = new Vector<String>();
    private Vector<String>                 passiveWitnesses          = new Vector<String>();

    // Set consisting of union of all known members.
    private HashMap<String, ClusterMember> potentialQuorumMembersSet = new HashMap<String, ClusterMember>();

    // Witness host definition, if used.
    private HashMap<String, ClusterMember> passsiveWitnessSet        = new HashMap<String, ClusterMember>();

    private StringBuilder                  decisionSteps             = new StringBuilder();

    private String                         conclusion                = "UNKNOWN";

    /**
     * Instantiates a digest used to compute whether the member that creates the
     * digest is in a primary group.
     * 
     * @param name Name of this member
     * @param configuredDBMembers Member names from service configuration
     * @param configuredActiveWitnesses Member names of active witnesses, if any
     * @param viewDBMembers Member names from group communications view that are
     *            DB members
     * @param viewActiveWitnessMembers Active witnesses that appear in the view
     *            provided by group communications
     * @param passiveWitnesses Names of the witness hosts
     */
    public ClusterMembershipDigest(String name,
            Collection<String> configuredDBMembers,
            Collection<String> configuredActiveWitnesses,
            Collection<String> viewDBMembers,
            Collection<String> viewActiveWitnessMembers,
            List<String> passiveWitnesses)
    {
        // Assign values.
        this.name = name;
        if (configuredDBMembers != null)
        {
            this.configuredDBMembers.addAll(configuredDBMembers);
        }
        if (configuredActiveWitnesses != null
                && configuredActiveWitnesses.size() > 0)
        {
            this.configuredActiveWitnesses.addAll(configuredActiveWitnesses);
        }
        if (viewDBMembers != null)
        {
            this.viewDBMembers.addAll(viewDBMembers);
            this.consolidatedViewMembers.addAll(viewDBMembers);
        }
        if (viewActiveWitnessMembers != null)
        {
            this.viewActiveWitnessMembers.addAll(viewActiveWitnessMembers);
            this.consolidatedViewMembers.addAll(viewActiveWitnessMembers);
        }
        if (passiveWitnesses != null)
        {
            this.passiveWitnesses.addAll(passiveWitnesses);
        }

        // Construct quorum set.
        derivePotentialQuorumMembersSet();
    }

    /*
     * Construct the set of all members, which is the union of the configured
     * and view members, that could potentially be in the quorum set.
     */
    private void derivePotentialQuorumMembersSet()
    {
        // Add configured members first.
        for (String name : configuredDBMembers)
        {
            ClusterMember cm = new ClusterMember(name);
            cm.setConfigured(true);
            cm.setDbMember(true);
            potentialQuorumMembersSet.put(name, cm);
        }

        if (configuredActiveWitnesses != null
                && configuredActiveWitnesses.size() > 0)
        {
            // Also add, if they exist, active witnesses.
            for (String name : configuredActiveWitnesses)
            {
                ClusterMember cm = new ClusterMember(name);
                cm.setConfigured(true);
                cm.setActiveWitness(true);
                potentialQuorumMembersSet.put(name, cm);
            }
        }

        /*
         * Now iterate across the view DB members and add new member definitions
         * or update existing ones.
         */
        for (String name : viewDBMembers)
        {
            ClusterMember cm = potentialQuorumMembersSet.get(name);
            if (cm == null)
            {
                cm = new ClusterMember(name);
                cm.setInView(true);
                potentialQuorumMembersSet.put(name, cm);
            }
            else
            {
                cm.setInView(true);
            }
        }

        /*
         * Now iterate across the view active witness members and add new member
         * definitions or update existing ones.
         */
        for (String name : viewActiveWitnessMembers)
        {
            ClusterMember cm = potentialQuorumMembersSet.get(name);
            if (cm == null)
            {
                cm = new ClusterMember(name);
                cm.setInView(true);
                potentialQuorumMembersSet.put(name, cm);
            }
            else
            {
                cm.setInView(true);
            }
        }

        // Add the witness hosts if we have any.
        for (String name : passiveWitnesses)
        {
            ClusterMember witness = new ClusterMember(name);
            witness.setPassiveWitness(true);
            passsiveWitnessSet.put(name, witness);
            potentialQuorumMembersSet.put(name, witness);
        }
    }

    /** Return name of current member. */
    public String getName()
    {
        return name;
    }

    /**
     * Return the number of members required to have a simple majority. We don't
     * count either active or passive witnesses when looking for a simple
     * majority but simply the number of DB members.
     */
    public int getSimpleMajoritySize()
    {
        return ((potentialQuorumMembersSet.size()
                - passiveWitnessesInQuorumSetCount() - activeWitnessesInQuorumSetCount()) / 2 + 1);
    }

    /**
     * Sets the validation flag on a member.
     * 
     * @param member Name of the member that was tested
     * @param valid If true member was validated through GC ping
     */
    public void setValidated(String member, boolean valid)
            throws ClusterMembershipValidationException
    {
        ClusterMember cm = potentialQuorumMembersSet.get(member);

        if (cm == null)
        {
            throw new ClusterMembershipValidationException(
                    String.format(
                            "Cannot validate member '%s' because it does not appear in the potential quorum member set.",
                            member));
        }

        if (!consolidatedViewMembers.contains(cm.getName()))
        {
            throw new ClusterMembershipValidationException(
                    String.format(
                            "Cannot validate member '%s' because it does not appear in the view.",
                            cm.getName()));
        }

        if (cm != null)
        {
            if (cm.getValidated() == valid)
            {
                return;
            }

            cm.setValidated(valid);
        }
    }

    /**
     * Sets the reachability flag on a member.
     * 
     * @param member Name of the member that was tested
     * @param reached If true member was reached with a network ping command
     */
    public void setReachable(String member, boolean reached)
    {
        ClusterMember cm = potentialQuorumMembersSet.get(member);
        if (cm != null)
        {
            if (cm.getReachable() == reached)
                return;

            cm.setReachable(reached);
        }
        else
        {
            ClusterMember witness = passsiveWitnessSet.get(member);
            if (member.equals(witness.getName()))
            {
                if (witness.getReachable() == reached)
                    return;

                witness.setReachable(reached);
            }
        }
    }

    /**
     * Return quorum set members.
     */
    public List<ClusterMember> getPotentialQuorumMembersSet()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                potentialQuorumMembersSet.size());
        list.addAll(potentialQuorumMembersSet.values());
        return list;
    }

    /**
     * Return definitions of the configured members.
     */
    public List<ClusterMember> getConsolidatedConfiguredMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                configuredDBMembers.size() + configuredActiveWitnesses.size());
        for (String name : configuredDBMembers)
        {
            list.add(potentialQuorumMembersSet.get(name));
        }
        for (String name : configuredActiveWitnesses)
        {
            list.add(potentialQuorumMembersSet.get(name));
        }
        return list;
    }

    /**
     * Return definitions of the configured members.
     */
    public List<ClusterMember> getConfiguredDBSetMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                configuredDBMembers.size());
        for (String name : configuredDBMembers)
        {
            list.add(potentialQuorumMembersSet.get(name));
        }
        return list;
    }

    /**
     * Return definitions of the configured members.
     */
    public List<ClusterMember> getConfiguredActiveWitnessSetMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                configuredActiveWitnesses.size());
        for (String name : configuredActiveWitnesses)
        {
            list.add(potentialQuorumMembersSet.get(name));
        }
        return list;
    }

    /**
     * Return definitions of the view members.
     */
    public List<ClusterMember> getViewDBSetMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                viewDBMembers.size());
        for (String name : viewDBMembers)
        {
            list.add(potentialQuorumMembersSet.get(name));
        }
        return list;
    }

    /**
     * Return definitions of the view members.
     */
    public List<ClusterMember> getViewActiveWitnessSetMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                viewActiveWitnessMembers.size());
        for (String name : viewActiveWitnessMembers)
        {
            list.add(potentialQuorumMembersSet.get(name));
        }
        return list;
    }

    /**
     * Return definitions of the witness members.
     */
    public List<ClusterMember> getWitnessSetMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                passsiveWitnessSet.size());
        for (ClusterMember cm : passsiveWitnessSet.values())
        {
            list.add(cm);
        }
        return list;
    }

    /**
     * Return the validated members.
     */
    public List<ClusterMember> getValidatedMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                potentialQuorumMembersSet.size());
        for (ClusterMember cm : potentialQuorumMembersSet.values())
        {
            // Validated members must have been checked *and* must have
            // a true value.
            Boolean valid = cm.getValidated();
            if (valid != null && valid)
                list.add(cm);
        }
        return list;
    }

    /**
     * Return the validated members.
     */
    public List<ClusterMember> getValidatedDBMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                getPotentialQuorumMembersSet().size());
        for (ClusterMember cm : potentialQuorumMembersSet.values())
        {
            if (cm.isActiveWitness() || cm.isPassiveWitness())
            {
                continue;
            }
            // Validated members must have been checked *and* must have
            // a true value.
            Boolean valid = cm.getValidated();
            if (valid != null && valid)
                list.add(cm);
        }
        return list;
    }

    /**
     * Return the validated members.
     */
    public List<ClusterMember> getValidatedActiveWitnessMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                activeWitnessesInQuorumSetCount());
        for (ClusterMember cm : potentialQuorumMembersSet.values())
        {
            if (!cm.isActiveWitness())
            {
                continue;
            }
            // Validated members must have been checked *and* must have
            // a true value.
            Boolean valid = cm.getValidated();
            if (valid != null && valid)
                list.add(cm);
        }
        return list;
    }

    /**
     * Return the reachable members.
     */
    public List<ClusterMember> getReachableMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                potentialQuorumMembersSet.size());
        for (ClusterMember cm : potentialQuorumMembersSet.values())
        {
            // Reachable members must have been checked *and* must have
            // a true value.
            Boolean reachable = cm.getReachable();
            if (reachable != null && reachable)
                list.add(cm);
        }
        return list;
    }

    /**
     * Return the reachable members.
     */
    public List<ClusterMember> getReachableDBMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                validatedDBMembersInQuorumSetCount());
        for (ClusterMember cm : potentialQuorumMembersSet.values())
        {
            if (cm.isActiveWitness() || cm.isPassiveWitness())
            {
                continue;
            }
            // Reachable members must have been checked *and* must have
            // a true value.
            Boolean reachable = cm.getReachable();
            if (reachable != null && reachable)
                list.add(cm);
        }
        return list;
    }

    /**
     * Return the reachable members.
     */
    public List<ClusterMember> getReachableActiveWitnessMembers()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                activeWitnessesInQuorumSetCount());
        for (ClusterMember cm : potentialQuorumMembersSet.values())
        {
            if (!cm.isActiveWitness())
            {
                continue;
            }
            // Reachable members must have been checked *and* must have
            // a true value.
            Boolean reachable = cm.getReachable();
            if (reachable != null && reachable)
                list.add(cm);
        }
        return list;
    }

    /**
     * Return the reachable witnesses.
     */
    public List<ClusterMember> getReachablePassiveWitnesses()
    {
        ArrayList<ClusterMember> list = new ArrayList<ClusterMember>(
                reachablePassiveWitnessesInQuorumSetCount());
        for (ClusterMember cm : passsiveWitnessSet.values())
        {
            // Reachable witnesses must have been checked *and* must have
            // a true value.
            Boolean reachable = cm.getReachable();
            if (reachable != null && reachable)
                list.add(cm);
        }
        return list;
    }

    /** Return member names from the quorum set. */
    public List<String> getPotentialQuorumMembersSetNames()
    {
        return clusterMembersToNames(potentialQuorumMembersSet.values());
    }

    /** Return validated member names. */
    public List<String> getValidatedMemberNames()
    {
        return clusterMembersToNames(getValidatedMembers());
    }

    /** Return validated member names. */
    public List<String> getValidatedDBMemberNames()
    {
        return clusterMembersToNames(getValidatedDBMembers());
    }

    /** Return validated member names. */
    public List<String> getValidatedActiveWitnessMemberNames()
    {
        return clusterMembersToNames(getValidatedActiveWitnessMembers());
    }

    /** Return reachable member names. */
    public List<String> getReachableDBMemberNames()
    {
        return clusterMembersToNames(getReachableDBMembers());
    }

    /** Return reachable member names. */
    public List<String> getReachableActiveWitnessMemberNames()
    {
        return clusterMembersToNames(getReachableActiveWitnessMembers());
    }

    /** Return reachable member names. */
    public List<String> getReachableMemberNames()
    {
        return clusterMembersToNames(getReachableMembers());
    }

    /** Return reachable witness names. */
    public List<String> getReachableWitnessNames()
    {
        return clusterMembersToNames(getReachablePassiveWitnesses());
    }

    // Conversion routine.
    private List<String> clusterMembersToNames(Collection<ClusterMember> members)
    {
        ArrayList<String> list = new ArrayList<String>(members.size());
        for (ClusterMember member : members)
        {
            list.add(member.getName());
        }
        return list;
    }

    /**
     * Test to see if we have a valid set of potential quorum members. This
     * checks a number of conditions that if violated indicate that the manager
     * is either misconfigured or group communications is misbehaving, which in
     * turn could lead to an invalid computation of quorum.
     * 
     * @return Returns true if the
     */
    public boolean isValidPotentialQuorumMembersSet(boolean verbose)
    {
        if (configuredDBMembers.size() == 0)
        {
            // The quorum set must contain at least one configured member.
            if (verbose)
            {
                logDecisionStep(verbose,
                        "INVALID POTENTIAL QUORUM MEMBERS SET: NO CONFIGURED MEMBERS FOUND");
                logDecisionStep(verbose,
                        "(ENSURE THAT dataservices.properties FILE CONTAINS AT LEAST MEMBER "
                                + name + ")");
            }
            return false;
        }
        else if (viewDBMembers.size() == 0)
        {
            // The quorum set must contain at least one member in the GC view.
            if (verbose)
            {
                logDecisionStep(
                        verbose,
                        "INVALID POTENTIAL QUORUM MEMBERS SET: GROUP COMMUNICATION VIEW CONTAINS NO MEMBERS");
                logDecisionStep(verbose,
                        "(GROUP COMMUNICATIONS MAY BE MISCONFIGURED OR BLOCKED BY A FIREWALL)");
            }
            return false;
        }
        else if (potentialQuorumMembersSet.get(name) == null)
        {
            // The quorum set must contain the current member.
            if (verbose)
            {
                logDecisionStep(verbose,
                        "INVALID POTENTIAL QUORUM MEMBERS SET: THIS MEMBER "
                                + name + " IS NOT LISTED");
                logDecisionStep(
                        verbose,
                        "(GROUP COMMUNICATIONS MAY BE MISCONFIGURED OR BLOCKED BY A FIREWALL; MEMBER NAME MAY BE MISSING FROM dataservices.properties)");
            }
            return false;
        }
        else if (!potentialQuorumMembersSet.get(name).isInView())
        {
            // The member must be in the group communications view.
            if (verbose)
            {
                logDecisionStep(
                        verbose,
                        "INVALID POTENTIAL QUORUM MEMBERS SET: THIS MEMBER "
                                + name
                                + " IS NOT LISTED IN THE GROUP COMMUNICATION VIEW");
                logDecisionStep(verbose,
                        "(GROUP COMMUNICATIONS MAY BE MISCONFIGURED OR BLOCKED BY A FIREWALL)");
            }
            return false;
        }
        else
        {
            // This quorum set appears valid.
            return true;
        }
    }

    /**
     * Determines whether the local manager is in a primary partition, based on
     * validated membership information passed in when this class is
     * instantiated. A manager is in a primary partition if one of the following
     * conditions is met.
     * <ul>
     * <li>The quorum set is one and contains the current member</li>
     * <li>The quorum set contains a simple majority of validated members</li>
     * <li>The quorum set contains an even number of validated members with
     * reachable witness hosts (all must be reachable)</li>
     * </ul>
     * If none of the above obtains, the manager is not in a primary partition.
     * 
     * @param verbose Logs information about how the determination is being
     *            made.
     * @return true if we are in a primary partition
     */
    public boolean isInPrimaryPartition(boolean verbose)
    {

        int simpleMajority = this.getSimpleMajoritySize();

        // Print a message to explain what we are doing.
        if (verbose)
        {
            logDecisionStep(verbose,
                    "========================================================================");
            if (passiveWitnessesInQuorumSetCount() > 0)
            {
                logDecisionStep(
                        verbose,
                        String.format(
                                "CHECKING FOR QUORUM: MUST BE AT LEAST %d DB MEMBERS %s",
                                simpleMajority,
                                simpleMajority > 1
                                        ? String.format(
                                                "OR %d MEMBERS PLUS ALL %d PASSIVE WITNESSES",
                                                simpleMajority - 1,
                                                passiveWitnessesInQuorumSetCount())
                                        : ""));
            }
            else if (activeWitnessesInQuorumSetCount() > 0)
            {
                logDecisionStep(
                        verbose,
                        String.format(
                                "CHECKING FOR QUORUM: MUST BE AT LEAST %d DB MEMBERS %s",
                                simpleMajority,
                                simpleMajority > 1
                                        ? String.format(
                                                "OR %d DB MEMBERS PLUS AT LEAST 1 ACTIVE WITNESS",
                                                simpleMajority - 1)
                                        : ""));
            }
            else
            {
                logDecisionStep(verbose, String.format(
                        "CHECKING FOR QUORUM: MUST BE AT LEAST %d DB MEMBERS",
                        simpleMajority));
            }
            logDecisionStep(
                    verbose,
                    "QUORUM SET MEMBERS ARE: "
                            + CLUtils
                                    .iterableToCommaSeparatedList(getPotentialQuorumMembersSetNames()));
            logDecisionStep(verbose,
                    "SIMPLE MAJORITY SIZE: " + this.getSimpleMajoritySize());

            logDecisionStep(verbose, "GC VIEW OF CURRENT DB MEMBERS IS: "
                    + CLUtils.iterableToCommaSeparatedList(viewDBMembers));
            logDecisionStep(
                    verbose,
                    "VALIDATED DB MEMBERS ARE: "
                            + CLUtils
                                    .iterableToCommaSeparatedList(getValidatedDBMemberNames()));
            logDecisionStep(
                    verbose,
                    "REACHABLE DB MEMBERS ARE: "
                            + CLUtils
                                    .iterableToCommaSeparatedList(getReachableDBMemberNames()));

            if (activeWitnessesInQuorumSetCount() > 0)
            {
                logDecisionStep(
                        verbose,
                        "GC VIEW OF CURRENT ACTIVE WITNESS MEMBERS IS: "
                                + CLUtils
                                        .iterableToCommaSeparatedList(viewDBMembers));
                logDecisionStep(
                        verbose,
                        "VALIDATED ACTIVE WITNESS MEMBERS ARE: "
                                + CLUtils
                                        .iterableToCommaSeparatedList(getValidatedActiveWitnessMemberNames()));
                logDecisionStep(
                        verbose,
                        "REACHABLE ACTIVE WITNESS MEMBERS ARE: "
                                + CLUtils
                                        .iterableToCommaSeparatedList(getReachableActiveWitnessMemberNames()));
            }

            if (passiveWitnesses.size() > 0)
            {
                logDecisionStep(
                        verbose,
                        "WITNESS HOSTS ARE: "
                                + CLUtils
                                        .iterableToCommaSeparatedList(passiveWitnesses));
                logDecisionStep(
                        verbose,
                        "REACHABLE WITNESSES ARE: "
                                + CLUtils
                                        .iterableToCommaSeparatedList(getReachableWitnessNames()));
            }

            logDecisionStep(verbose,
                    "========================================================================");
        }

        // Ensure the quorum set is valid.
        if (!this.isValidPotentialQuorumMembersSet(verbose))
        {
            logDecisionStep(
                    verbose,
                    "CONCLUSION: UNABLE TO ESTABLISH MAJORITY DUE TO INVALID POTENTIAL QUORUM MEMBERS SET");
            return false;
        }

        if (!this.isValidMembership(verbose))
        {
            logDecisionStep(verbose, "CONCLUSION: MEMBERSHIP IS INVALID");
            return false;
        }

        /*
         * If we have a valid quorum set with a single validated member, then we
         * have a primary partition. This case covers a cluster with a single
         * master.
         */
        if (potentialQuorumMembersSet.size() == 1
                && validatedDBMembersInQuorumSetCount() == 1)
        {
            logDecisionStep(
                    verbose,
                    "CONCLUSION: I AM IN A PRIMARY PARTITION AS THERE IS A SINGLE VALIDATED MEMBER IN THE QUORUM SET");
            return true;
        }

        // If we have a simple majority of validated members in the quorum set,
        // then we have a primary partition.
        if (validatedDBMembersInQuorumSetCount() >= simpleMajority)
        {
            logDecisionStep(
                    verbose,
                    String.format(
                            "CONCLUSION: I AM IN A PRIMARY PARTITION OF %d DB MEMBERS OUT OF THE REQUIRED MAJORITY OF %d",
                            validatedDBMembersInQuorumSetCount(),
                            simpleMajority));
            return true;
        }

        /*
         * By the time we get here, 'validated' should be equal to 'viewMembers'
         * since we will return the fact that the the potential quorum members
         * set is invalid if they are not. So the test that uses 'validated'
         * below should be sufficient to indicated that all members can be seen.
         * If we are shy of a majority by one member, we can use witnesses, if
         * they exist, to break the tie. But the key is that if there is more
         * than one witness, we need to see ALL of the witnesses. Otherwise we
         * could end up with a partition in which one partition sees one witness
         * and the other partition sees another etc.
         */
        if (validatedDBMembersInQuorumSetCount() >= simpleMajority - 1)
        {
            boolean passiveWitnessesOK = passsiveWitnessSet.size() > 0
                    && (passsiveWitnessSet.size() == reachablePassiveWitnessesInQuorumSetCount());

            if (passiveWitnessesOK)
            {
                logDecisionStep(
                        verbose,
                        String.format(
                                "CONCLUSION: I AM IN A PRIMARY PARTITION OF %d MEMBERS, WITH %d VALIDATED DB MEMBERS AND ALL (%d) REACHABLE PASSIVE WITNESSES",
                                simpleMajority,
                                validatedDBMembersInQuorumSetCount(),
                                reachablePassiveWitnessesInQuorumSetCount()));
                return true;
            }
            else
            {
                /*
                 * If we have active witnesses, check them now....
                 */
                int validatedActiveWitneses = validatedActiveWitnessesInQuorumSetCount();

                if (validatedDBMembersInQuorumSetCount()
                        + validatedActiveWitneses >= simpleMajority)
                {
                    logDecisionStep(
                            verbose,
                            String.format(
                                    "CONCLUSION: I AM IN A PRIMARY PARTITION WITH %d VALIDATED DB MEMBERS AND %d VALIDATED ACTIVE WITNESSES",
                                    validatedDBMembersInQuorumSetCount(),
                                    validatedActiveWitneses));
                    return true;
                }
                else
                {
                    logDecisionStep(
                            verbose,
                            String.format(
                                    "CONCLUSION: I AM IN A NON-PRIMARY PARTITION OF %d MEMBERS OUT OF A REQUIRED MAJORITY SIZE OF %d\n"
                                            + "AND THERE ARE %d REACHABLE WITNESSES OUT OF %d",
                                    validatedDBMembersInQuorumSetCount(),
                                    getSimpleMajoritySize(),
                                    reachablePassiveWitnessesInQuorumSetCount(),
                                    passsiveWitnessSet.size()));
                    return false;
                }
            }
        }

        /*
         * We cannot form a quorum. Provide an explanation if desired.
         */
        if (verbose)
        {
            logDecisionStep(
                    verbose,
                    String.format(
                            "CONCLUSION: I AM IN A NON-PRIMARY PARTITION OF %d MEMBERS OUT OF A REQUIRED MAJORITY SIZE OF %d\n",
                            validatedDBMembersInQuorumSetCount(),
                            getSimpleMajoritySize()));
        }
        return false;
    }

    /**
     * Returns true if the group membership is valid, which is the case if the
     * following conditions obtain:
     * <ul>
     * <li>There is at least 1 member in the group</li>
     * <li>All individual members in the group are validated through a ping</li>
     * </ul>
     */
    public boolean isValidMembership(boolean verbose)
    {
        /*
         * This is a case where we are looking for consistency between the total
         * view that we see via GCS and the members that we can validate. So
         * here we do not treat active witnesses any differently because what we
         * are testing for is, essentially, consistency between what GCS sees
         * and the members that are reachable. If we don't have a consistent
         * view, then there's possibly a network partition in effect etc.
         */
        if (consolidatedViewMembers.size() > 0
                && getValidatedMembers().size() > 0)
        {
            if (setsAreEqual(consolidatedViewMembers, getValidatedMembers()))
            {

                if (verbose)
                {
                    logDecisionStep(verbose,
                            "MEMBERSHIP IS VALID BASED ON VIEW/VALIDATED CONSOLIDATED MEMBERS CONSISTENCY");
                }
                return true;
            }
        }

        if (verbose)
        {
            logDecisionStep(
                    verbose,
                    String.format(
                            "MEMBERSHIP IS NOT VALID: %d MEMBERS APPEAR IN VIEW AND ONLY %d CAN BE VALIDATED",
                            consolidatedViewMembers.size(),
                            getValidatedMembers().size()));

        }
        return false;
    }

    private boolean setsAreEqual(List<String> viewSet,
            List<ClusterMember> targetSet)
    {
        int hitCount = 0;

        for (String viewMember : viewSet)
        {
            for (ClusterMember member : targetSet)
            {
                if (member.getName().equals(viewMember))
                    hitCount++;
            }
        }

        return (hitCount == viewSet.size());
    }

    public Vector<String> getConfiguredActiveWitnesses()
    {
        return configuredActiveWitnesses;
    }

    public void setConfiguredActiveWitnesses(
            Vector<String> configuredActiveWitnesses)
    {
        this.configuredActiveWitnesses = configuredActiveWitnesses;
    }

    /**
     * Determine how many active witnesses are in the potential quorum set.
     */
    public int activeWitnessesInQuorumSetCount()
    {
        int activeWitnessCount = 0;

        for (ClusterMember member : potentialQuorumMembersSet.values())
        {
            if (member.isActiveWitness())
            {
                activeWitnessCount++;
            }
        }

        return activeWitnessCount;
    }

    public int validatedActiveWitnessesInQuorumSetCount()
    {
        int validatedActiveWitnessCount = 0;

        for (ClusterMember member : potentialQuorumMembersSet.values())
        {
            if (member.isActiveWitness() && member.getValidated())
            {
                validatedActiveWitnessCount++;
            }
        }

        return validatedActiveWitnessCount;
    }

    public int reachablePassiveWitnessesInQuorumSetCount()
    {
        int count = 0;

        for (ClusterMember member : potentialQuorumMembersSet.values())
        {
            if (member.isPassiveWitness() && member.getReachable())
            {
                count++;
            }
        }

        return count;
    }

    public int validatedDBMembersInQuorumSetCount()
    {
        int validatedDBMemberCount = 0;

        for (ClusterMember member : potentialQuorumMembersSet.values())
        {
            if (member.isDbMember() && member.getValidated())
            {
                validatedDBMemberCount++;
            }
        }

        return validatedDBMemberCount;
    }

    /**
     * Determine how many passive witnesses are in the potential quorum set.
     */
    public int passiveWitnessesInQuorumSetCount()
    {
        int passiveWitnessCount = 0;

        for (ClusterMember member : potentialQuorumMembersSet.values())
        {
            if (member.isPassiveWitness())
            {
                passiveWitnessCount++;
            }
        }

        return passiveWitnessCount;
    }

    public Vector<String> getViewActiveWitnessMembers()
    {
        return viewActiveWitnessMembers;
    }

    public void setViewActiveWitnessMembers(
            Vector<String> viewActiveWitnessMembers)
    {
        this.viewActiveWitnessMembers = viewActiveWitnessMembers;
    }

    public Vector<String> getConsolidatedViewMembers()
    {
        return consolidatedViewMembers;
    }

    public void setConsolidatedViewMembers(
            Vector<String> consolidatedViewMembers)
    {
        this.consolidatedViewMembers = consolidatedViewMembers;
    }

    private void logDecisionStep(boolean verbose, String step)
    {
        decisionSteps.append(step).append("\n");
        if (step.startsWith("CONCLUSION"))
        {
            setConclusion(step);
        }
        if (verbose)
        {
            CLUtils.println(step);
        }
    }

    public String getDecisionSteps()
    {
        return decisionSteps.toString();
    }

    public String getConclusion()
    {
        return conclusion;
    }

    public void setConclusion(String conclusion)
    {
        this.conclusion = conclusion;
    }

    public boolean isValidated(String name)
            throws ClusterMembershipValidationException
    {
        ClusterMember cm = potentialQuorumMembersSet.get(name);

        if (cm == null)
        {
            throw new ClusterMembershipValidationException(
                    String.format(
                            "Member '%s' does not exist in the potential quorum member set",
                            name));
        }

        return cm.getValidated();
    }
}
