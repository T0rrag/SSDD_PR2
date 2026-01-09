/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service;

import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.Vector;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Hosts;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.Recipe;
import recipes_service.data.Recipes;
import recipes_service.data.RemoveOperation;
import recipes_service.tsae.data_structures.Log;
import recipes_service.tsae.data_structures.Timestamp;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import recipes_service.tsae.sessions.TSAESessionOriginatorSide;
/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class ServerData {
	
	// server id
	private String id;
	
	// sequence number of the last recipe timestamped by this server
	private long seqnum=Timestamp.NULL_TIMESTAMP_SEQ_NUMBER; // sequence number (to timestamp)

	// timestamp lock
	private Object timestampLock = new Object();
	
	// TSAE data structures
	private Log log = null;
	private TimestampVector summary = null;
	private TimestampMatrix ack = null;
	
	// recipes data structure
	private Recipes recipes = new Recipes();

	// number of TSAE sessions
	int numSes = 1; // number of different partners that a server will contact for a TSAE session each time that TSAE timer (each sessionPeriod seconds) expires

	// propDegree: (default value: 0) number of TSAE sessions done each time a new data is created
	int propDegree = 0;
	
	// Participating nodes
	private Hosts participants;

	// TSAE timers
	private long sessionDelay;
	private long sessionPeriod = 10;

	private Timer tsaeSessionTimer;

	//
	TSAESessionOriginatorSide tsae = null;

	// TODO: esborrar aquesta estructura de dades
	// tombstones: timestamp of removed operations
	private static class Tombstone implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		private Timestamp recipeTimestamp;
		private Timestamp removeTimestamp;
		public Tombstone(Timestamp recipeTimestamp, Timestamp removeTimestamp) {
			this.recipeTimestamp = recipeTimestamp;
			this.removeTimestamp = removeTimestamp;
		}
		public Timestamp getRecipeTimestamp() {
			return recipeTimestamp;
		}
		public Timestamp getRemoveTimestamp() {
			return removeTimestamp;
		}
		public void setRemoveTimestamp(Timestamp removeTimestamp) {
			this.removeTimestamp = removeTimestamp;
		}
	}
	private List<Tombstone> tombstones = new Vector<Tombstone>();
	
	// end: true when program should end; false otherwise
	private boolean end;

	private final Object serverConnectedLock = new Object();
	private boolean serverConnected = false;

	public ServerData(){
	}
	
private synchronized void purgeTombstones() {
	    if (ack == null) {
	        return;
	    }
	    
	    // Obtener el vector con los timestamps mínimos conocidos por todos
	    TimestampVector sum = ack.minTimestampVector();
	    
	    if (sum == null) {
	        return;
	    }
	    
	    // Crear una nueva lista para tombstones que deben mantenerse
	    List<Tombstone> newTombstones = new Vector<Tombstone>();
	    
	    for (int i = 0; i < tombstones.size(); i++) {
	        Tombstone tombstone = tombstones.get(i);
	        Timestamp removeTs = tombstone.getRemoveTimestamp();
	        if (removeTs == null) {
	            newTombstones.add(tombstone);
	            continue;
	        }
	        Timestamp minTs = sum.getLast(removeTs.getHostid());
	        
	        // Mantener el tombstone si su timestamp es mayor que el mínimo conocido
	        if (minTs != null && removeTs.compare(minTs) > 0) {
	            newTombstones.add(tombstone);
	        }
	        // Si removeTs.compare(minTs) <= 0, significa que todos conocen
	        // la eliminación, por lo que se puede purgar
	    }
	    
	    tombstones = newTombstones;
	    
	    LSimLogger.log(Level.TRACE, 
	        "Tombstones purged. Remaining: " + tombstones.size());
	}
	
	/**
	 * Starts the execution
	 * @param participantss
	 */
	public void startTSAE(Hosts participants){
		this.participants = participants;
		this.log = new Log(participants.getIds());
		this.summary = new TimestampVector(participants.getIds());
		this.ack = new TimestampMatrix(participants.getIds());
		

		//  Sets the Timer for TSAE sessions
	    tsae = new TSAESessionOriginatorSide(this);
		tsaeSessionTimer = new Timer();
		tsaeSessionTimer.scheduleAtFixedRate(tsae, sessionDelay, sessionPeriod);
	}

	public void stopTSAEsessions(){
		this.tsaeSessionTimer.cancel();
	}
	
	public boolean end(){
		return this.end;
	}
	
	public void setEnd(){
		this.end = true;
	}

	// ******************************
	// *** timestamps
	// ******************************
	private Timestamp nextTimestamp(){
		Timestamp nextTimestamp = null;
		synchronized (timestampLock){
			if (seqnum == Timestamp.NULL_TIMESTAMP_SEQ_NUMBER){
				seqnum = -1;
			}
			nextTimestamp = new Timestamp(id, ++seqnum);
		}
		return nextTimestamp;
	}

	// ******************************
	// *** add and remove recipes
	// ******************************
	public synchronized void addRecipe(String recipeTitle, String recipe) {

		Timestamp timestamp= nextTimestamp();
		Recipe rcpe = new Recipe(recipeTitle, recipe, id, timestamp);
		Operation op=new AddOperation(rcpe, timestamp);

		if (this.log.add(op)) {
			this.summary.updateTimestamp(timestamp);
			if (this.ack != null) {
				this.ack.update(id, summary);
			}
			this.recipes.add(rcpe);
		}
//		LSimLogger.log(Level.TRACE,"The recipe '"+recipeTitle+"' has been added");

	}
	
public synchronized void removeRecipe(String recipeTitle) {
	    // Obtener la receta actual (si existe)
		Recipe recipe = recipes.get(recipeTitle);
		if (recipe == null) {
			return;
		}

		Timestamp removeTimestamp = nextTimestamp();
		
		Timestamp recipeTimestamp = recipe.getTimestamp();
		Operation op = new RemoveOperation(recipeTitle, recipeTimestamp, removeTimestamp);
	   

		// Añadir al log
		//this.log.add(op);
	    
		if (this.log.add(op)) {
			// Actualizar summary
			this.summary.updateTimestamp(removeTimestamp);
		    
			if (this.ack != null) {
				this.ack.update(id, summary);
			}
		    
			// Eliminar de la colección de recetas
			this.recipes.remove(recipeTitle);
		    
			// Añadir a tombstones (para evitar readiciones)
			addTombstone(recipeTimestamp, removeTimestamp);
		    
			LSimLogger.log(Level.TRACE, 
				"Recipe '" + recipeTitle + "' has been removed. Timestamp: " + removeTimestamp);
		}
	}

/**
 * Método auxiliar para aplicar una operación RemoveOperation recibida
 * Debe ser llamado desde TSAESessionOriginatorSide y TSAESessionPartnerSide
 */
private void applyRemoveOperationReceived(RemoveOperation removeOp) {
	String recipeTitle = removeOp.getRecipeTitle();
	Timestamp recipeTimestamp = removeOp.getRecipeTimestamp();
	Timestamp removeTimestamp = removeOp.getTimestamp();
	
	Recipe recipe = recipes.get(recipeTitle);
	
	// Verificar si la receta existe y tiene el mismo timestamp
	if (recipe != null && recipe.getTimestamp().equals(recipeTimestamp)) {
		// Eliminar la receta
		recipes.remove(recipeTitle);
		
		// Añadir a tombstones
		addTombstone(recipeTimestamp, removeTimestamp);
		
		LSimLogger.log(Level.TRACE, 
			"Applied RemoveOperation for recipe: " + recipeTitle);
	} else if (recipe == null) {
		// La receta no existe - puede que ya fue eliminada o aún no añadida
		// Guardar en tombstones para prevenir adición futura
		addTombstone(recipeTimestamp, removeTimestamp);
	}
}

/**
 * Método auxiliar para aplicar una operación AddOperation recibida
 * Debe verificar tombstones antes de añadir
 */
private void applyAddOperationReceived(AddOperation addOp) {
	Recipe recipe = addOp.getRecipe();
	
	// Verificar si esta receta fue eliminada (está en tombstones)
	if (!isTombstoned(recipe.getTimestamp())) {
		// Añadir la receta solo si no está en tombstones
		recipes.add(recipe);
		LSimLogger.log(Level.TRACE, 
			"Applied AddOperation for recipe: " + recipe.getTitle());
	} else {
		LSimLogger.log(Level.TRACE, 
			"Skipped AddOperation for removed recipe: " + recipe.getTitle());
	}
}
// ****************************************************************************
// *** operations to get the TSAE data structures. Used to send to evaluation
// ****************************************************************************
public Log getLog() {
	return log;
}
public TimestampVector getSummary() {
	return summary;
}
public TimestampMatrix getAck() {
	return ack;
}
public Recipes getRecipes(){
	return recipes;
}

public synchronized TimestampVector getSummaryClone() {
	return summary.clone();
}

public synchronized TimestampMatrix getAckClone() {
	return ack == null ? null : ack.clone();
}

public synchronized void refreshAck() {
	if (ack != null) {
		ack.update(id, summary);
	}
}

public synchronized void updateAckMax(TimestampMatrix otherAck) {
	if (ack != null && otherAck != null) {
		ack.updateMax(otherAck);
	}
}

public synchronized List<Operation> listNewerOperations(TimestampVector otherSummary) {
	return log.listNewer(otherSummary);
}

public synchronized void applyOperation(Operation op) {
	if (!log.add(op)) {
		return;
	}
	if (op.getType() == OperationType.ADD) {
		applyAddOperation((AddOperation) op);
	} else if (op.getType() == OperationType.REMOVE) {
		applyRemoveOperation((RemoveOperation) op);
	}
	summary.updateTimestamp(op.getTimestamp());
	if (ack != null) {
		ack.update(id, summary);
	}
}

public synchronized void purgeLog() {
	if (ack == null) {
		return;
	}
	log.purgeLog(ack);
	purgeTombstones();
}

private boolean isTombstoned(Timestamp timestamp) {
	for (Tombstone tombstone : tombstones) {
		if (tombstone != null && tombstone.getRecipeTimestamp() != null && tombstone.getRecipeTimestamp().equals(timestamp)) {
			return true;
		}
	}
	return false;
}

private void addTombstone(Timestamp timestamp) {
	addTombstone(timestamp, null);
}

private void addTombstone(Timestamp recipeTimestamp, Timestamp removeTimestamp) {
	if (recipeTimestamp == null) {
		return;
	}
	for (Tombstone tombstone : tombstones) {
		if (tombstone != null && tombstone.getRecipeTimestamp() != null && tombstone.getRecipeTimestamp().equals(recipeTimestamp)) {
			if (removeTimestamp != null) {
				Timestamp currentRemove = tombstone.getRemoveTimestamp();
				if (currentRemove == null || removeTimestamp.compare(currentRemove) > 0) {
					tombstone.setRemoveTimestamp(removeTimestamp);
				}
			}
			return;
		}
	}
	tombstones.add(new Tombstone(recipeTimestamp, removeTimestamp));
}

private void applyAddOperation(AddOperation op) {
	Recipe recipe = op.getRecipe();
	if (isTombstoned(recipe.getTimestamp())) {
		return;
	}
	Recipe existing = recipes.get(recipe.getTitle());
	if (existing == null || existing.getTimestamp().compare(recipe.getTimestamp()) < 0) {
		recipes.add(recipe);
	}
}

private void applyRemoveOperation(RemoveOperation op) {
	addTombstone(op.getRecipeTimestamp(), op.getTimestamp());
	Recipe existing = recipes.get(op.getRecipeTitle());
	if (existing != null && existing.getTimestamp().equals(op.getRecipeTimestamp())) {
		recipes.remove(op.getRecipeTitle());
	}
}

public void setSessionDelay(long sessionDelay){
	this.sessionDelay = sessionDelay;
}

public void setSessionPeriod(long sessionPeriod){
	this.sessionPeriod = sessionPeriod;
}

public List<Host> getRandomPartners(int num){
	List<Host> res = new Vector<Host>();
	if (participants == null || num <= 0) return res;
	try {
		java.lang.reflect.Method m = participants.getClass().getMethod("getRandomPartners", int.class, String.class);
		Object o = m.invoke(participants, num, id);
		if (o instanceof List) return (List<Host>) o;
	} catch (Exception e) { }
	try {
		java.lang.reflect.Method m = participants.getClass().getMethod("getRandomPartners", int.class);
		Object o = m.invoke(participants, num);
		if (o instanceof List) return (List<Host>) o;
	} catch (Exception e) { }
	try {
		java.lang.reflect.Method m = participants.getClass().getMethod("getRandomHosts", int.class, String.class);
		Object o = m.invoke(participants, num, id);
		if (o instanceof List) return (List<Host>) o;
	} catch (Exception e) { }
	try {
		java.lang.reflect.Method m = participants.getClass().getMethod("getRandomHosts", int.class);
		Object o = m.invoke(participants, num);
		if (o instanceof List) return (List<Host>) o;
	} catch (Exception e) { }
	try {
		java.lang.reflect.Method m = participants.getClass().getMethod("getHosts");
		Object o = m.invoke(participants);
		if (o instanceof List) {
			List<?> all = (List<?>) o;
			for (Object h : all) {
				if (h instanceof Host) res.add((Host) h);
			}
		}
	} catch (Exception e) { }
	if (res.size() > num) {
		return res.subList(0, num);
	}
	return res;
}

public void notifyServerConnected() {
	synchronized (serverConnectedLock) {
		serverConnected = true;
		serverConnectedLock.notifyAll();
	}
}

public void waitServerConnected() {
	synchronized (serverConnectedLock) {
		while (!serverConnected) {
			try {
				serverConnectedLock.wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
}

public TSAESessionOriginatorSide getTSAESessionOriginatorSide() {
	return tsae;
}

// ******************************
// *** getters and setters
// ******************************
public void setId(String id){
	this.id = id;		
}
public String getId(){
	return this.id;
}

public int getNumberSessions(){
	return numSes;
}

public void setNumberSessions(int numSes){
	this.numSes = numSes;
}

public int getPropagationDegree(){
	return this.propDegree;
}

public void setPropagationDegree(int propDegree){
	this.propDegree = propDegree;
}
}