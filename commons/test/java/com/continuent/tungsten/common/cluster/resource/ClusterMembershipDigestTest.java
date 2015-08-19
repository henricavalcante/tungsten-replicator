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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.common.cluster.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.junit.Test;

import com.continuent.tungsten.common.exception.ClusterMembershipValidationException;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Implements a unit test of IndexedLRUCache features.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ClusterMembershipDigestTest
{
    /**
     * Verify that we can create a membership digest instance that returns a
     * correctly computed quorum set and witnesses.
     */
    @Test
    public void testInstantiationNormal() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b", "c");
        List<String> viewActiveWitnesses = null;
        List<String> witnesses = null;
    
        ClusterMembershipDigest digest = new ClusterMembershipDigest("myname",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // Test that values are correctly returned.
        List<String> quorumSet = Arrays.asList("a", "b", "c");
        Assert.assertEquals("myname", digest.getName());
        Assert.assertEquals("Configured member size", 3, digest
                .getConfiguredDBSetMembers().size());
        Assert.assertEquals("Configured view size", 3, digest
                .getViewDBSetMembers().size());
        Assert.assertEquals("Witness set size", 0, digest
                .getWitnessSetMembers().size());
        assertEqualSet("Quorum set", quorumSet,
                digest.getPotentialQuorumMembersSetNames());
    }

    /**
     * Verify that we can create a membership digest instance that returns a
     * correctly computed quorum set and witnesses.
     */
    @Test
    public void testInstantiationActiveWitnesses() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b");
        List<String> configuredActiveWitnessMembers = Arrays.asList("c");
        List<String> viewDBMembers = Arrays.asList("a", "b");
        List<String> viewActiveWitnesses = Arrays.asList("c");
        ;
        List<String> witnesses = null;
       
        ClusterMembershipDigest digest = new ClusterMembershipDigest("myname",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // Test that values are correctly returned.
        List<String> quorumSet = Arrays.asList("a", "b", "c");
        Assert.assertEquals("myname", digest.getName());
        Assert.assertEquals("Configured member size", 2, digest
                .getConfiguredDBSetMembers().size());
        Assert.assertEquals("Configured DB view size", 2, digest
                .getViewDBSetMembers().size());
        Assert.assertEquals("Configured Active Witness view size", 1, digest
                .getViewActiveWitnessSetMembers().size());
        Assert.assertEquals("Witness set size", 0, digest
                .getWitnessSetMembers().size());
        assertEqualSet("Quorum set", quorumSet,
                digest.getPotentialQuorumMembersSetNames());
    }

    /**
     * Verify that we can create a membership digest instance that returns a
     * correctly computed quorum set and witnesses.
     */
    @Test
    public void testInstantiationPassiveWitnesses() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b", "c");
        List<String> viewActiveWitnesses = null;
        List<String> witnesses = Arrays.asList("d");
        
        ClusterMembershipDigest digest = new ClusterMembershipDigest("myname",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // Test that values are correctly returned.
        List<String> quorumSet = Arrays.asList("a", "b", "c", "d");
        Assert.assertEquals("myname", digest.getName());
        Assert.assertEquals("Configured member size", 3, digest
                .getConfiguredDBSetMembers().size());
        Assert.assertEquals("Configured view size", 3, digest
                .getViewDBSetMembers().size());
        Assert.assertEquals("Witness set size", 1, digest
                .getWitnessSetMembers().size());
        assertEqualSet("Quorum set", quorumSet,
                digest.getPotentialQuorumMembersSetNames());
    }

    /**
     * Verify that the digest correctly indicates membership is valid if all
     * members are validated and otherwise returns false.
     */
    @Test
    public void testMembershipValidity() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b");
        List<String> view = Arrays.asList("a", "b", "c", "d");
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configured, null, view, null, null);

        // Assert that the membership is invalid as long as not all members are
        // validated.
        for (String member : view)
        {
            Assert.assertFalse("Before member validated: " + member,
                    digest.isValidMembership(false));
            digest.setValidated(member, true);
        }

        // Now it should be valid.
        Assert.assertTrue("All members are valid",
                digest.isValidMembership(false));
    }

    /**
     * Verify that a quorum set of one validated node is a primary partition.
     */
    @Test
    public void testMajorityOfOne() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a");
        List<String> viewActiveWitnesses = null;
        List<String> witnesses = null;
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // If we have not validated the node, we don't have a majority.
        Assert.assertFalse("Unvalidated member cannot create majority",
                digest.isInPrimaryPartition(true));

        // Once the node is validated, we have a majority.
        digest.setValidated("a", true);
        Assert.assertTrue("Single validated member constitutes a majority",
                digest.isInPrimaryPartition(true));
    }

    /**
     * Verify that a quorum set is a primary partition if there is a simple
     * majority of nodes where all nodes in the view are validated.
     */
    @Test
    public void testSimpleMajority() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b");
        List<String> viewActiveWitnesses = null;
        List<String> witnesses = null;
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // If we have not validated a majority, we are not in a primary
        // partition.
        Assert.assertFalse("0 of 3 validated is not majority",
                digest.isInPrimaryPartition(true));

        // I need to be sure that 'myself' is validated. But that's not enough
        // for a majority.
        CLUtils.println("About to set validated for a");
        digest.setValidated("a", true);
        Assert.assertFalse("1 of 3 validated is not majority",
                digest.isInPrimaryPartition(true));

        // 2 of 3 is a majority but only if the view is valid.
        digest.setValidated("b", true);
        Assert.assertTrue("2 of 3 validated is a majority",
                digest.isInPrimaryPartition(true));

        /*
         * Test for a majority of three out of four configured members.
         */
        configuredDBMembers = Arrays.asList("a", "b", "c", "d");
        viewDBMembers = Arrays.asList("a", "b", "c");
        digest = new ClusterMembershipDigest("a", configuredDBMembers,
                configuredActiveWitnessMembers, viewDBMembers,
                viewActiveWitnesses, witnesses);

        // I need to be sure that 'myself' is validated. But that's not enough
        // for a majority.
        digest.setValidated("a", true);
        Assert.assertFalse("1 of 4 validated is not majority",
                digest.isInPrimaryPartition(true));

        // 2 of 4 is not a majority.
        digest.setValidated("b", true);
        Assert.assertFalse("2 of 4 validated is not a majority",
                digest.isInPrimaryPartition(true));

        // 3 of 4 is a majority.
        digest.setValidated("c", true);
        Assert.assertTrue("3 of 4 validated is a majority",
                digest.isInPrimaryPartition(true));

    }

    /**
     * Verify that a quorum set with an even number of validate nodes plus a
     * reachable witness is a primary partition.
     */
    @Test
    public void testWitness() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a");
        List<String> viewActiveWitnesses = null;
        List<String> witnesses = Arrays.asList("d");

        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // Always validate ourself, but that is not enough for a quorum...
        digest.setValidated("a", true);
        Assert.assertFalse(
                "1 of 2 validated without witnesses is not majority",
                digest.isInPrimaryPartition(true));

        digest.setReachable("a", true);
        Assert.assertFalse(
                "1 of 2 validated without all witnesses reachable is not majority",
                digest.isInPrimaryPartition(true));

        // 1 of 2 with all reachable witnesses is a majority.
        digest.setReachable("d", true);
        Assert.assertTrue(
                "1 of 2 validated with all witnesses reachable is majority",
                digest.isInPrimaryPartition(true));

        // To be thorough ensure we properly fail if witnesses are null.
        digest = new ClusterMembershipDigest("a", configuredDBMembers,
                configuredActiveWitnessMembers, viewDBMembers,
                viewActiveWitnesses, null);

        digest.setValidated("a", true);
        Assert.assertFalse(
                "1 of 2 validated with null witnesses is not majority",
                digest.isInPrimaryPartition(true));

        /*
         * Add one more witness and then test to make sure that a primary
         * partition only exists if 1 of 2 members is present plus ALL passive
         * witnesses.
         */
        witnesses = Arrays.asList("d", "e");
        digest = new ClusterMembershipDigest("a", configuredDBMembers,
                configuredActiveWitnessMembers, viewDBMembers,
                viewActiveWitnesses, witnesses);

        // Always validate ourself, but that is not enough for a quorum...
        digest.setValidated("a", true);
        Assert.assertFalse(
                "1 of 2 validated without witnesses is not majority",
                digest.isInPrimaryPartition(true));

        digest.setReachable("a", true);
        Assert.assertFalse(
                "1 of 2 validated without all witnesses reachable is not majority",
                digest.isInPrimaryPartition(true));

        // 1 of 2 with all reachable witnesses is a majority.
        digest.setReachable("d", true);
        Assert.assertFalse(
                "1 of 2 validated with only 1 of 2 witnesses is not a primary partition",
                digest.isInPrimaryPartition(true));

        digest.setReachable("e", true);
        Assert.assertTrue(
                "1 of 2 validated with 2 of 2 witnesses is a primary partition",
                digest.isInPrimaryPartition(true));

    }

    /**
     * Test to make sure that we detect inconsistent views i.e a view in which
     * all of the members cannot be verified. This type of issue occurs
     * frequently during a JGroups group transition where the process of a group
     * member has just exited but JGroups has not updated the internal view yet.
     * It has also happened, historically, due to bugs in JGroups in which the
     * protocol to remove members from the view was not working as designed.
     * 
     * @throws Exception
     */
    @Test
    public void testInconsistentViewDetection() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b", "c");
        List<String> viewActiveWitnesses = null;
        List<String> witnesses = null;
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        // If we have not validated a majority, we are not in a primary
        // partition.
        Assert.assertFalse("0 of 3 validated is not majority",
                digest.isInPrimaryPartition(true));

        // I need to be sure that 'myself' is validated. But that's not enough
        // for a majority.
        CLUtils.println("About to set validated for a");
        digest.setValidated("a", true);
        Assert.assertFalse("1 of 3 validated is not majority",
                digest.isInPrimaryPartition(true));

        /*
         * We should detect an inconsistent view here because only 2 of 3
         * members of the view are validated.
         */
        CLUtils.println("About to set validated for b");
        digest.setValidated("b", true);
        Assert.assertFalse("2 of 3 validated is a majority",
                digest.isInPrimaryPartition(true));

        CLUtils.println("About to set validated for c");
        digest.setValidated("c", true);
        Assert.assertTrue("3 of 3 validated is a majority",
                digest.isInPrimaryPartition(true));

    }

    @Test
    public void testAttemptToValidateMemberThatIsNotInView() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = Arrays.asList("d");
        List<String> viewDBMembers = Arrays.asList("a", "c");
        List<String> viewActiveWitnesses = Arrays.asList("d");
        List<String> witnesses = null;
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        digest.setValidated("a", true);

        Assert.assertTrue("Verified that 'a' is validated",
                digest.isValidated("a"));

        /*
         * Try to validate a member that does not exist at all.
         */
        try
        {
            digest.setValidated("doesNotExist", true);
        }
        catch (ClusterMembershipValidationException c)
        {
            Assert.assertTrue(
                    "Got an exception when validating a member that is not in the quorum set",
                    c.getMessage()
                            .equals("Cannot validate member 'doesNotExist' because it does not appear in the potential quorum member set."));
        }

        /*
         * Try to validate a member that does not exist in the view.
         */
        try
        {
            digest.setValidated("b", true);
        }
        catch (ClusterMembershipValidationException c)
        {
            Assert.assertTrue(
                    "Got an exception when validating a member that is not in the quorum set",
                    c.getMessage()
                            .equals("Cannot validate member 'b' because it does not appear in the view."));
        }

    }

    /**
     * Active witnesses should be used to establish a quorum only if a quorum
     * cannot be derived from existing DB members. Also, active witnesses should
     * never count in the initial calculation of the simple majority.
     * 
     * @throws Exception
     */
    @Test
    public void testUsingActiveWitnessOnlyIfNeeded() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = Arrays.asList("d");
        List<String> viewDBMembers = Arrays.asList("a", "d");
        List<String> viewActiveWitnesses = Arrays.asList("d");
        List<String> witnesses = null;
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, witnesses);

        /* simple majority should be 3/2 + 1 == 2 */
        Assert.assertTrue("simple majority is equal to 2",
                digest.getSimpleMajoritySize() == 2);

        /*
         * Validate one DB member and then test for a primary partition - should
         * fail.
         */
        digest.setValidated("a", true);
        Assert.assertFalse(
                "Only a single validated member - not in a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now validate the active witness and check for a primary partition. In
         * this case we should have a primary partition since simple majority=2
         * and one validated DB host plus one validated active witness == 2.
         */
        digest.setValidated("d", true);
        Assert.assertTrue(
                "One validated DB member and one validated active witness is a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Also check to be sure that the decision was made for the right reason
         * i.e. that we have 2 DB members as the majority.
         */
        Assert.assertTrue(
                "The conclusion was based solely on the 1 DB member and 1 active witness",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION WITH 1 VALIDATED DB MEMBERS AND 1 VALIDATED ACTIVE WITNESSES"));

        /*
         * Add a DB member to the view and validate both of them, then check for
         * a primary partition. Should still get a valid primary partition, but
         * should only use the DB members.
         */
        viewDBMembers = Arrays.asList("a", "b");
        viewActiveWitnesses = Arrays.asList("d");
        digest = new ClusterMembershipDigest("a", configuredDBMembers,
                configuredActiveWitnessMembers, viewDBMembers,
                viewActiveWitnesses, witnesses);
        digest.setValidated("a", true);
        digest.setValidated("b", true);
        digest.setValidated("d", true);

        Assert.assertTrue("Two validated DB members is a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Also check to be sure that the decision was made for the right reason
         * i.e. that we have 2 DB members as the majority.
         */
        Assert.assertTrue(
                "The conclusion was based solely on the 2 DB members",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION OF 2 DB MEMBERS OUT OF THE REQUIRED MAJORITY OF 2"));

        /*
         * Try everything with 4 members plus 2 active witnesses. The simple
         * majority should always be 3 and everything else should work
         * correctly.
         */
        configuredDBMembers = Arrays.asList("a", "b", "c", "d");
        configuredActiveWitnessMembers = Arrays.asList("e");
        witnesses = null;
        viewDBMembers = Arrays.asList("a", "b", "c", "d");
        viewActiveWitnesses = Arrays.asList("e");
        digest = new ClusterMembershipDigest("a", configuredDBMembers,
                configuredActiveWitnessMembers, viewDBMembers,
                viewActiveWitnesses, witnesses);
        digest.setValidated("a", true);
        digest.setValidated("b", true);

        /* simple majority should be 4/2 + 1 == 3 */
        Assert.assertTrue("simple majority is equal to 3",
                digest.getSimpleMajoritySize() == 3);

        Assert.assertFalse(
                "Two validated DB members is NOT a primary partition",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "NOT a primary partition because the view is inconsistent",
                digest.getConclusion()
                        .equals("CONCLUSION: MEMBERSHIP IS INVALID"));

        digest.setValidated("c", true);
        digest.setValidated("d", true);
        digest.setValidated("e", true);

        Assert.assertTrue("In a valid partition - 4 out of 4 DB members",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "In a primary partition because we have 4 validated DB members",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION OF 4 DB MEMBERS OUT OF THE REQUIRED MAJORITY OF 3"));

    }

    @Test
    public void testTwoDBNodesAndOnePassiveWitness() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b");
        List<String> viewActiveWitnesses = null;
        List<String> passiveWitnesses = Arrays.asList("c");

        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, passiveWitnesses);

        /* simple majority should be 2/2 + 1 == 2 */
        Assert.assertTrue("simple majority is equal to 2",
                digest.getSimpleMajoritySize() == 2);

        /*
         * Validate one DB member and then test for a primary partition - should
         * fail.
         */
        digest.setValidated("a", true);
        Assert.assertFalse(
                "Only a single validated member - not in a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now make the passive witness reachable and re-test for a primary
         * partition. Should fail because we have a problem with view/validated
         * consistency .
         */
        digest.setReachable("c", true);
        Assert.assertFalse(
                "One validated DB member and one reachable passive witness is not primary partition - invalid membership",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "We don't have a primary partition becaues of problems with the memberhship",
                digest.getConclusion()
                        .equals("CONCLUSION: MEMBERSHIP IS INVALID"));

        /*
         * Now make the second DB node validated. Should establish a primary
         * partition due to having two validated DB nodes.
         */
        digest.setValidated("b", true);

        Assert.assertTrue("Two validated DB members is a primary partition",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "We arrived at a primary partition for the correct reason - 2 validated DB members",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION OF 2 DB MEMBERS OUT OF THE REQUIRED MAJORITY OF 2"));

    }

    @Test
    public void testTwoDBNodesWithOneInViewAndTwoPassiveWitnesses()
            throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a");
        List<String> viewActiveWitnesses = null;
        List<String> passiveWitnesses = Arrays.asList("c", "d");

        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, passiveWitnesses);

        /* simple majority should be 2/2 + 1 == 2 */
        Assert.assertTrue("simple majority is equal to 2",
                digest.getSimpleMajoritySize() == 2);

        /*
         * Validate one DB member and then test for a primary partition - should
         * fail.
         */
        digest.setValidated("a", true);
        Assert.assertFalse(
                "Only a single validated member - not in a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now make the passive witness reachable and re-test for a primary
         * partition. Should fail to get a primary partition decision because
         * all witnesses must be reachable in this case.
         */
        digest.setReachable("c", true);
        Assert.assertFalse(
                "One validated DB member and one reachable passive witness is a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now set the second witness as reachable and re-test. Should succeed.
         */
        digest.setReachable("d", true);
        Assert.assertTrue(
                "One validated DB member and two (all) reachable passive witness is a primary partition",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "We arrived at a primary partition for the correct reason - 1 DB member and 1 passive witness",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION OF 2 MEMBERS, WITH 1 VALIDATED DB MEMBERS AND ALL (2) REACHABLE PASSIVE WITNESSES"));

    }

    @Test
    public void testTwoDBNodesWithTwoInViewAndOnePassiveWitness()
            throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b");
        List<String> viewActiveWitnesses = null;
        List<String> passiveWitnesses = Arrays.asList("c", "d");

        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, passiveWitnesses);

        /* simple majority should be 2/2 + 1 == 2 */
        Assert.assertTrue("simple majority is equal to 2",
                digest.getSimpleMajoritySize() == 2);

        /*
         * Validate one DB member and then test for a primary partition - should
         * fail.
         */
        digest.setValidated("a", true);
        Assert.assertFalse(
                "Only a single validated member - not in a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now make the passive witness reachable and re-test for a primary
         * partition. Should fail to get a primary partition decision because
         * all witnesses must be reachable in this case.
         */
        digest.setReachable("c", true);
        Assert.assertFalse(
                "One validated DB member and one reachable passive witness is a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now set the second witness as reachable and re-test. Should fail
         * because we still have an inconsistency between the view and validated
         * members.
         */
        digest.setReachable("d", true);
        Assert.assertFalse(
                "One validated DB member and two (all) reachable passive witness is still not a primary partition",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "We still don't have a primary partition because the membership IS INVALID",
                digest.getConclusion()
                        .equals("CONCLUSION: MEMBERSHIP IS INVALID"));

        /*
         * Now make the second DB node validated. Should establish a primary
         * partition due to having two validated DB nodes.
         */
        digest.setValidated("b", true);

        Assert.assertTrue("Two validated DB members is a primary partition",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "We arrived at a primary partition for the correct reason - 2 validated DB members",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION OF 2 DB MEMBERS OUT OF THE REQUIRED MAJORITY OF 2"));

    }

    @Test
    public void testThreeDBNodesAndOnePassiveWitness() throws Exception
    {
        List<String> configuredDBMembers = Arrays.asList("a", "b", "c");
        List<String> configuredActiveWitnessMembers = null;
        List<String> viewDBMembers = Arrays.asList("a", "b", "c");
        List<String> viewActiveWitnesses = null;
        List<String> passiveWitnesses = Arrays.asList("d");

        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configuredDBMembers, configuredActiveWitnessMembers,
                viewDBMembers, viewActiveWitnesses, passiveWitnesses);

        /* simple majority should be 3/2 + 1 == 2 */
        Assert.assertTrue("simple majority is equal to 2",
                digest.getSimpleMajoritySize() == 2);

        /*
         * Validate one DB member and then test for a primary partition - should
         * fail.
         */
        digest.setValidated("a", true);
        Assert.assertFalse(
                "Only a single validated member - not in a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Validate a second DB member and then test for a primary partition -
         * should fail because of view/validated member consistency problems.
         */
        digest.setValidated("b", true);
        Assert.assertFalse(
                "Two validated members out of a view of 3 - not in a primary partition",
                digest.isInPrimaryPartition(true));

        /*
         * Now flesh out the validated members and re-test. Should succeed.
         */
        digest.setValidated("c", true);

        Assert.assertTrue("Three validated DB members is a primary partition",
                digest.isInPrimaryPartition(true));

        Assert.assertTrue(
                "We arrived at a primary partition for the correct reason - 2 validated DB members",
                digest.getConclusion()
                        .equals("CONCLUSION: I AM IN A PRIMARY PARTITION OF 3 DB MEMBERS OUT OF THE REQUIRED MAJORITY OF 2"));

    }

    @Test
    public void testTwoDBNodesAndOneActiveWitness()
    {

    }

    @Test
    public void testThreeDBNodesAndOneActiveWitness()
    {

    }

    /**
     * Verify that invalid configurations are properly caught. This includes a
     * configuration where the member name is not included in the quorum set,
     * where the quorum set is empty, or where the view is empty.
     */
    @Test
    public void testInvalidConfigurations() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b");
        List<String> view = Arrays.asList("a", "b");

        // Confirm that member name must be in quorum set.
        ClusterMembershipDigest badMemberName = new ClusterMembershipDigest(
                "c", configured, view, null, view, view);
        Assert.assertFalse("Member must be in quorum set",
                badMemberName.isValidPotentialQuorumMembersSet(true));

        // Confirm that quorum set must be non-null.
        ClusterMembershipDigest emptyQuorum = new ClusterMembershipDigest("a",
                new ArrayList<String>(), null, null, view, view);
        Assert.assertFalse("Quorum set must non-null",
                emptyQuorum.isValidPotentialQuorumMembersSet(true));

        // Confirm that GC view contains members.
        ClusterMembershipDigest emptyView = new ClusterMembershipDigest("a",
                configured, null, null, view, view);
        Assert.assertFalse("View must have members",
                emptyView.isValidPotentialQuorumMembersSet(true));

        // Confirm that GC view contains the member name.
        ClusterMembershipDigest memberNotInView = new ClusterMembershipDigest(
                "a", configured, Arrays.asList("b"), null, view, view);
        Assert.assertFalse("View must contain the member name",
                memberNotInView.isValidPotentialQuorumMembersSet(true));

        // Confirm that configured list has at least one member.
        ClusterMembershipDigest noConfiguredNames = new ClusterMembershipDigest(
                "a", null, Arrays.asList("a"), null, view, view);
        Assert.assertFalse("Configured names must include at least one name",
                noConfiguredNames.isValidPotentialQuorumMembersSet(true));
    }

    // Assert that two lists contain identical members.
    private void assertEqualSet(String message, List<String> first,
            List<String> second)
    {
        Set<String> a = new TreeSet<String>(first);
        Set<String> b = new TreeSet<String>(second);
        Assert.assertTrue(message, a.equals(b));
    }
}