// src/identityService.ts
import { pool, Contact } from './database';

export interface IdentifyRequest {
  email?: string;
  phoneNumber?: string;
}

export interface IdentifyResponse {
  contact: {
    primaryContatctId: number; // Note: keeping the typo as per requirements
    emails: string[];
    phoneNumbers: string[];
    secondaryContactIds: number[];
  };
}

export class IdentityService {
  
  async identify(request: IdentifyRequest): Promise<IdentifyResponse> {
    const { email, phoneNumber } = request;
    
    if (!email && !phoneNumber) {
      throw new Error('Either email or phoneNumber must be provided');
    }

    // Find existing contacts
    const existingContacts = await this.findExistingContacts(email, phoneNumber);
    
    if (existingContacts.length === 0) {
      // Create new primary contact
      const newContact = await this.createPrimaryContact(email, phoneNumber);
      return this.buildResponse([newContact]);
    }

    // Get all linked contacts for each found contact
    const allLinkedContacts = await this.getAllLinkedContacts(existingContacts);
    
    // Check if we need to create a new secondary contact
    const needsNewContact = this.needsNewSecondaryContact(allLinkedContacts, email, phoneNumber);
    
    if (needsNewContact) {
      const primaryContact = this.findOldestPrimary(allLinkedContacts);
      const newSecondary = await this.createSecondaryContact(email, phoneNumber, primaryContact.id);
      allLinkedContacts.push(newSecondary);
    }

    // Handle potential merging of separate contact chains
    const finalContacts = await this.handleContactChainMerging(allLinkedContacts, email, phoneNumber);
    
    return this.buildResponse(finalContacts);
  }

  private async findExistingContacts(email?: string, phoneNumber?: string): Promise<Contact[]> {
    const conditions = [];
    const params = [];
    let paramIndex = 1;

    if (email) {
      conditions.push(`email = $${paramIndex++}`);
      params.push(email);
    }
    
    if (phoneNumber) {
      conditions.push(`phoneNumber = $${paramIndex++}`);
      params.push(phoneNumber);
    }

    const query = `
      SELECT * FROM Contact 
      WHERE (${conditions.join(' OR ')}) 
      AND deletedAt IS NULL
      ORDER BY createdAt ASC
    `;

    const result = await pool.query(query, params);
    return result.rows;
  }

  private async getAllLinkedContacts(contacts: Contact[]): Promise<Contact[]> {
    if (contacts.length === 0) return [];

    // Get all primary contact IDs
    const primaryIds = new Set<number>();
    
    for (const contact of contacts) {
      if (contact.linkPrecedence === 'primary') {
        primaryIds.add(contact.id);
      } else if (contact.linkedId) {
        primaryIds.add(contact.linkedId);
      }
    }

    // Fetch all contacts linked to these primaries
    const primaryIdArray = Array.from(primaryIds);
    const placeholders = primaryIdArray.map((_, i) => `$${i + 1}`).join(',');
    
    const query = `
      SELECT * FROM Contact 
      WHERE (id IN (${placeholders}) OR linkedId IN (${placeholders}))
      AND deletedAt IS NULL
      ORDER BY createdAt ASC
    `;

    const result = await pool.query(query, [...primaryIdArray, ...primaryIdArray]);
    return result.rows;
  }

  private needsNewSecondaryContact(existingContacts: Contact[], email?: string, phoneNumber?: string): boolean {
    // Check if the exact combination already exists
    return !existingContacts.some(contact => 
      contact.email === (email || null) && 
      contact.phoneNumber === (phoneNumber || null)
    );
  }

  private findOldestPrimary(contacts: Contact[]): Contact {
    return contacts
      .filter(c => c.linkPrecedence === 'primary')
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())[0];
  }

  private async handleContactChainMerging(contacts: Contact[], email?: string, phoneNumber?: string): Promise<Contact[]> {
    // Group contacts by their primary
    const contactGroups = new Map<number, Contact[]>();
    
    for (const contact of contacts) {
      const primaryId = contact.linkPrecedence === 'primary' ? contact.id : contact.linkedId!;
      if (!contactGroups.has(primaryId)) {
        contactGroups.set(primaryId, []);
      }
      contactGroups.get(primaryId)!.push(contact);
    }

    // If we have multiple groups, we need to merge them
    if (contactGroups.size > 1) {
      const primaryGroups = Array.from(contactGroups.entries());
      
      // Find the oldest primary to keep as the main primary
      const oldestPrimary = primaryGroups
        .map(([_, contacts]) => contacts.find(c => c.linkPrecedence === 'primary')!)
        .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())[0];

      // Update all other primaries to be secondary
      for (const [primaryId, groupContacts] of primaryGroups) {
        if (primaryId !== oldestPrimary.id) {
          await this.convertPrimaryToSecondary(primaryId, oldestPrimary.id);
          
          // Update the in-memory contacts as well
          for (const contact of groupContacts) {
            if (contact.linkPrecedence === 'primary') {
              contact.linkPrecedence = 'secondary';
              contact.linkedId = oldestPrimary.id;
            } else {
              contact.linkedId = oldestPrimary.id;
            }
          }
        }
      }

      // Re-fetch all contacts to get the updated state
      return this.getAllLinkedContactsById(oldestPrimary.id);
    }

    return contacts;
  }

  private async convertPrimaryToSecondary(primaryId: number, newPrimaryId: number): Promise<void> {
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Update the primary contact to secondary
      await client.query(
        'UPDATE Contact SET linkedId = $1, linkPrecedence = $2, updatedAt = CURRENT_TIMESTAMP WHERE id = $3',
        [newPrimaryId, 'secondary', primaryId]
      );
      
      // Update all contacts that were linked to the old primary
      await client.query(
        'UPDATE Contact SET linkedId = $1, updatedAt = CURRENT_TIMESTAMP WHERE linkedId = $2',
        [newPrimaryId, primaryId]
      );
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  private async getAllLinkedContactsById(primaryId: number): Promise<Contact[]> {
    const query = `
      SELECT * FROM Contact 
      WHERE (id = $1 OR linkedId = $1)
      AND deletedAt IS NULL
      ORDER BY createdAt ASC
    `;

    const result = await pool.query(query, [primaryId]);
    return result.rows;
  }

  private async createPrimaryContact(email?: string, phoneNumber?: string): Promise<Contact> {
    const query = `
      INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence, createdAt, updatedAt)
      VALUES ($1, $2, null, 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
    `;

    const result = await pool.query(query, [phoneNumber || null, email || null]);
    return result.rows[0];
  }

  private async createSecondaryContact(email?: string, phoneNumber?: string, linkedId?: number): Promise<Contact> {
    const query = `
      INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence, createdAt, updatedAt)
      VALUES ($1, $2, $3, 'secondary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
    `;

    const result = await pool.query(query, [phoneNumber || null, email || null, linkedId]);
    return result.rows;
  }

  private buildResponse(contacts: Contact[]): IdentifyResponse {
    const primary = contacts.find(c => c.linkPrecedence === 'primary')!;
    const secondaries = contacts.filter(c => c.linkPrecedence === 'secondary');

    const emails = Array.from(new Set([
      primary.email,
      ...secondaries.map(c => c.email)
    ].filter(Boolean) as string[]));

    const phoneNumbers = Array.from(new Set([
      primary.phoneNumber,
      ...secondaries.map(c => c.phoneNumber)
    ].filter(Boolean) as string[]));

    return {
      contact: {
        primaryContatctId: primary.id,
        emails,
        phoneNumbers,
        secondaryContactIds: secondaries.map(c => c.id)
      }
    };
  }
}
